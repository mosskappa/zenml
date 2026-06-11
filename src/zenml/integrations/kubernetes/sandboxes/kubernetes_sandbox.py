"""Kubernetes sandbox implementation."""

import logging
import queue
import shlex
import threading
import time
import uuid
from typing import Dict, Iterator, List, Optional, Type, Union, cast

from kubernetes import client as k8s_client
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream as k8s_stream

from zenml.config.base_settings import BaseSettings
from zenml.integrations.kubernetes import kube_utils
from zenml.integrations.kubernetes.flavors import (
    KubernetesSandboxConfig,
    KubernetesSandboxSettings,
)
from zenml.integrations.kubernetes.manifest_utils import build_pod_manifest
from zenml.logger import get_logger
from zenml.sandboxes.base import BaseSandbox, BaseSandboxSettings
from zenml.sandboxes.process import SandboxExecError, SandboxProcess
from zenml.sandboxes.session import SandboxSession

logger = get_logger(__name__)

_STREAM_END = object()


class KubernetesSandboxProcess(SandboxProcess):
    """Handle to a command running in a Kubernetes sandbox session."""

    def __init__(
        self,
        session: "KubernetesSandboxSession",
        websocket_client: "k8s_stream.ws_client.WSClient",
        started_at: float,
    ) -> None:
        """Initialize the Kubernetes sandbox process.

        Args:
            session: The owning sandbox session.
            websocket_client: Kubernetes websocket client for pod exec.
            started_at: The wall-clock time the process started.
        """
        super().__init__(session=session, started_at=started_at)
        self._session = session
        self._websocket_client = websocket_client
        self._stdout_queue: "queue.Queue[object]" = queue.Queue()
        self._stderr_queue: "queue.Queue[object]" = queue.Queue()
        self._exit_code: Optional[int] = None
        self._done = threading.Event()
        self._drain_thread = threading.Thread(
            target=self._drain_streams,
            name=f"k8s-sandbox-process-{session.id}",
            daemon=True,
        )
        self._drain_thread.start()

    @staticmethod
    def _split_complete_lines(
        chunk: str, buffer: str
    ) -> tuple[List[str], str]:
        """Split a chunk into complete lines and trailing buffer.

        Args:
            chunk: A chunk read from the websocket stream.
            buffer: The trailing incomplete line from previous chunks.

        Returns:
            A tuple of complete lines and remaining trailing buffer.
        """
        combined = f"{buffer}{chunk}"
        lines = combined.splitlines(keepends=True)
        if lines and not lines[-1].endswith("\n"):
            return lines[:-1], lines[-1]
        return lines, ""

    def _drain_streams(self) -> None:
        """Read stdout/stderr from websocket and feed line queues."""
        stdout_buffer = ""
        stderr_buffer = ""
        try:
            while self._websocket_client.is_open():
                self._websocket_client.update(timeout=1)
                if self._websocket_client.peek_stdout():
                    stdout_chunk = self._websocket_client.read_stdout()
                    lines, stdout_buffer = self._split_complete_lines(
                        stdout_chunk, stdout_buffer
                    )
                    for line in lines:
                        self._stdout_queue.put(line)
                if self._websocket_client.peek_stderr():
                    stderr_chunk = self._websocket_client.read_stderr()
                    lines, stderr_buffer = self._split_complete_lines(
                        stderr_chunk, stderr_buffer
                    )
                    for line in lines:
                        self._stderr_queue.put(line)
        except Exception as e:
            logger.debug(
                "Error while draining Kubernetes sandbox streams: %s", e
            )
            if self._exit_code is None:
                self._exit_code = 1
            self._stderr_queue.put(
                f"Kubernetes sandbox stream failed: {e}\n"
            )
        finally:
            if stdout_buffer:
                self._stdout_queue.put(stdout_buffer)
            if stderr_buffer:
                self._stderr_queue.put(stderr_buffer)
            if self._exit_code is None:
                self._exit_code = self._websocket_client.returncode
            if self._exit_code is None:
                self._exit_code = 1
            self._stdout_queue.put(_STREAM_END)
            self._stderr_queue.put(_STREAM_END)
            self._done.set()

    @staticmethod
    def _iter_queue(q: "queue.Queue[object]") -> Iterator[str]:
        """Iterate queue items until sentinel is reached.

        Args:
            q: Queue of stream line items.

        Yields:
            Stream lines.
        """
        while True:
            item = q.get()
            if item is _STREAM_END:
                break
            yield cast(str, item)

    def stdout(self) -> Iterator[str]:
        """Stdout line iterator.

        Returns:
            Stdout line iterator.
        """
        return self._session._wrap_stream(
            self._iter_queue(self._stdout_queue), log_level=logging.INFO
        )

    def stderr(self) -> Iterator[str]:
        """Stderr line iterator.

        Returns:
            Stderr line iterator.
        """
        return self._session._wrap_stream(
            self._iter_queue(self._stderr_queue), log_level=logging.ERROR
        )

    def wait(self, timeout: Optional[float] = None) -> int:
        """Wait for command completion.

        Args:
            timeout: Timeout in seconds to wait.

        Raises:
            TimeoutError: If the command did not finish in time.

        Returns:
            The exit code.
        """
        if not self._done.wait(timeout):
            raise TimeoutError("Timed out waiting for sandbox command to finish.")
        assert self._exit_code is not None
        return self._exit_code

    def kill(self) -> None:
        """Terminate the running command stream."""
        if not self._done.is_set():
            try:
                self._websocket_client.close()
            except Exception as e:
                logger.warning(
                    "KubernetesSandbox kill() failed: %s. Command stream may "
                    "still be active.",
                    e,
                    exc_info=True,
                )

    @property
    def exit_code(self) -> Optional[int]:
        """Exit code or `None` if still running.

        Returns:
            Exit code or `None`.
        """
        return self._exit_code


class KubernetesSandboxSession(SandboxSession):
    """Session for a Kubernetes-backed sandbox pod."""

    def __init__(
        self,
        *,
        id: str,
        pod_name: str,
        namespace: str,
        parent: "BaseSandbox",
    ) -> None:
        """Initialize a Kubernetes sandbox session.

        Args:
            id: Session identifier.
            pod_name: Name of the backing Kubernetes pod.
            namespace: Kubernetes namespace of the pod.
            parent: The sandbox component that created this session.
        """
        self._pod_name = pod_name
        self._namespace = namespace
        super().__init__(id=id, parent=parent)

    @property
    def _core_api(self) -> k8s_client.CoreV1Api:
        """Return Kubernetes Core API client.

        Returns:
            The CoreV1Api client.
        """
        sandbox = cast(KubernetesSandbox, self._parent)
        return sandbox.core_api

    @staticmethod
    def _build_shell_command(
        command: Union[str, List[str]],
        cwd: Optional[str],
        env: Optional[Dict[str, str]],
    ) -> List[str]:
        """Build shell command for pod exec with cwd/env handling.

        Args:
            command: Command to execute.
            cwd: Optional working directory.
            env: Optional environment variables.

        Returns:
            A shell command list suitable for Kubernetes pod exec.
        """
        if isinstance(command, list):
            command_str = " ".join(shlex.quote(part) for part in command)
        else:
            command_str = command

        fragments: List[str] = []
        if cwd:
            fragments.append(f"cd {shlex.quote(cwd)}")

        if env:
            exports = " ".join(
                f"{key}={shlex.quote(value)}"
                for key, value in env.items()
            )
            fragments.append(f"export {exports}")

        fragments.append(command_str)
        script = " && ".join(fragments)
        return ["/bin/sh", "-c", script]

    def _exec(
        self,
        command: Union[str, List[str]],
        *,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> SandboxProcess:
        """Execute a command in the sandbox pod.

        Args:
            command: The command to execute.
            cwd: Optional working directory override.
            env: Optional environment variables to set in the command process.

        Raises:
            SandboxExecError: If command execution cannot be started.

        Returns:
            Process handle.
        """
        self._log_command(command)

        exec_command = self._build_shell_command(command, cwd, env)
        started_at = time.time()
        try:
            websocket_client = k8s_stream(
                self._core_api.connect_get_namespaced_pod_exec,
                self._pod_name,
                self._namespace,
                command=exec_command,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
                _preload_content=False,
            )
        except Exception as e:
            raise SandboxExecError(
                f"Kubernetes sandbox execution failed to launch: {e}"
            ) from e

        return KubernetesSandboxProcess(
            session=self,
            websocket_client=websocket_client,
            started_at=started_at,
        )

    def _close(self) -> None:
        """Close session handle without terminating the pod."""

    def _destroy(self) -> None:
        """Delete the backing sandbox pod from Kubernetes."""
        sandbox = cast(KubernetesSandbox, self._parent)
        try:
            kube_utils.retry_on_api_exception(
                self._core_api.delete_namespaced_pod,
                api_request_timeout=sandbox.config.api_request_timeout,
            )(
                name=self._pod_name,
                namespace=self._namespace,
                propagation_policy="Foreground",
            )
        except ApiException as e:
            if e.status != 404:
                raise


class KubernetesSandbox(BaseSandbox):
    """Kubernetes pod-backed sandbox."""

    _k8s_client: Optional[k8s_client.ApiClient] = None

    @property
    def config(self) -> KubernetesSandboxConfig:
        """Kubernetes sandbox configuration.

        Returns:
            The Kubernetes sandbox configuration.
        """
        return cast(KubernetesSandboxConfig, self._config)

    @property
    def settings_class(self) -> Optional[Type["BaseSettings"]]:
        """Settings class.

        Returns:
            `KubernetesSandboxSettings`.
        """
        return KubernetesSandboxSettings

    def get_kube_client(self) -> k8s_client.ApiClient:
        """Get the Kubernetes API client.

        Returns:
            The Kubernetes API client.

        Raises:
            RuntimeError: If the service connector returns an unexpected client.
        """
        if self.config.incluster:
            kube_utils.load_kube_config(incluster=True)
            self._k8s_client = k8s_client.ApiClient()
            return self._k8s_client

        if self._k8s_client and not self.connector_has_expired():
            return self._k8s_client

        connector = self.get_connector()
        if connector:
            client = connector.connect()
            if not isinstance(client, k8s_client.ApiClient):
                raise RuntimeError(
                    f"Expected a k8s_client.ApiClient while trying to use "
                    f"the linked connector, but got {type(client)}."
                )
            self._k8s_client = client
        else:
            kube_utils.load_kube_config(
                context=self.config.kubernetes_context,
            )
            self._k8s_client = k8s_client.ApiClient()

        return self._k8s_client

    @property
    def core_api(self) -> k8s_client.CoreV1Api:
        """Get the Kubernetes Core API client.

        Returns:
            The CoreV1Api client.
        """
        return k8s_client.CoreV1Api(self.get_kube_client())

    def create_session(
        self, settings: Optional[BaseSandboxSettings] = None
    ) -> SandboxSession:
        """Create a sandbox session backed by a Kubernetes pod.

        Args:
            settings: Optional settings overrides.

        Returns:
            A Kubernetes sandbox session.
        """
        resolved_settings = cast(
            KubernetesSandboxSettings, self.resolve_settings(settings)
        )
        if (
            resolved_settings.require_service_account
            and not resolved_settings.service_account_name
        ):
            raise ValueError(
                "Kubernetes sandbox requires `service_account_name` when "
                "`require_service_account` is enabled."
            )

        session_id = f"k8s-{uuid.uuid4().hex[:12]}"
        pod_name = kube_utils.sanitize_label(f"zenml-sandbox-{session_id}")
        labels = {
            "zenml-sandbox-id": kube_utils.sanitize_label_value(session_id),
            "zenml-sandbox-component-id": kube_utils.sanitize_label_value(
                str(self.id)
            ),
        }
        env = self._resolve_session_environment(resolved_settings)
        pod_manifest = build_pod_manifest(
            pod_name=pod_name,
            image_name=resolved_settings.image,
            command=["/bin/sh", "-c"],
            args=["while true; do sleep 30; done"],
            privileged=resolved_settings.privileged,
            pod_settings=resolved_settings.pod_settings,
            service_account_name=resolved_settings.service_account_name,
            env=env,
            labels=labels,
        )
        if pod_manifest.spec is not None:
            pod_manifest.spec.automount_service_account_token = (
                resolved_settings.automount_service_account_token
            )

        kube_utils.retry_on_api_exception(
            self.core_api.create_namespaced_pod,
            api_request_timeout=resolved_settings.api_request_timeout,
        )(
            namespace=self.config.kubernetes_namespace,
            body=pod_manifest,
        )

        try:
            kube_utils.wait_pod(
                kube_client_fn=self.get_kube_client,
                pod_name=pod_name,
                namespace=self.config.kubernetes_namespace,
                exit_condition_lambda=lambda pod: (
                    pod.status is not None
                    and pod.status.phase == kube_utils.PodPhase.RUNNING.value
                ),
                timeout_sec=resolved_settings.startup_timeout_seconds,
                api_request_timeout=resolved_settings.api_request_timeout,
            )
        except Exception:
            try:
                self.core_api.delete_namespaced_pod(
                    name=pod_name,
                    namespace=self.config.kubernetes_namespace,
                    propagation_policy="Foreground",
                )
            except Exception:
                logger.debug(
                    "Failed to clean up Kubernetes sandbox pod `%s` "
                    "after startup failure.",
                    pod_name,
                    exc_info=True,
                )
            raise

        return KubernetesSandboxSession(
            id=session_id,
            pod_name=pod_name,
            namespace=self.config.kubernetes_namespace,
            parent=self,
        )
