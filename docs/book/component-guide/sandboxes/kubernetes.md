---
description: Pod-backed sandbox sessions for isolated command execution on Kubernetes.
---

# Kubernetes Sandbox

The Kubernetes sandbox flavor creates one Kubernetes pod per sandbox session and executes each `session.exec(...)` command inside that pod using Kubernetes exec streaming.

Compared to the local sandbox, this flavor provides infrastructure-level isolation boundaries (namespace, service account, pod security settings) and is suitable for running untrusted or semi-trusted generated code when your cluster policies are configured appropriately.

## How to register

```bash
zenml integration install kubernetes
zenml sandbox register k8s-sb --flavor=kubernetes
zenml stack update --sandbox k8s-sb
```

## Settings

The Kubernetes sandbox inherits `BaseSandboxSettings` and adds Kubernetes-specific controls:

- `sandbox_environment`: environment variables injected into sandbox pods and command executions.
- `image`: container image used for session pods (pin digest in production).
- `pod_settings`: optional `KubernetesPodSettings` overrides (resources, tolerations, labels, volumes, and more).
- `service_account_name`: service account used by sandbox pods.
- `require_service_account` (default: `True`): fails session creation unless `service_account_name` is set.
- `automount_service_account_token` (default: `False`): controls whether a Kubernetes API token is mounted in the pod.
- `privileged` (default: `False`): whether to run containers in privileged mode.
- `startup_timeout_seconds`: max wait for the pod to become `Running`.
- `api_request_timeout`: timeout for Kubernetes API requests.

The stack component config additionally supports:

- `kubernetes_namespace`
- `kubernetes_context`
- `incluster`

Use a Kubernetes service connector or local kubeconfig credentials exactly as you would for other Kubernetes stack components.

## Security model for LLM-generated code

This flavor is designed to make safer defaults possible, but cluster policy remains the primary security boundary.

### ZenML-side safeguards (default behavior)

- Service account must be explicitly set (`require_service_account=True`).
- Service account token is not mounted in sandbox pods (`automount_service_account_token=False`).

### Required cluster-admin controls

For untrusted LLM code, configure at least:

- **Least-privilege RBAC** for sandbox service accounts.
- **Pod Security Admission** (or equivalent policy engine) to forbid privileged containers and host namespace escapes.
- **Runtime hardening policy** (e.g. Kyverno/Gatekeeper) to enforce non-root, read-only root filesystem, seccomp, and no privilege escalation where required.
- **Network policies** with explicit egress/ingress rules (default-deny plus allow-list).
- **ResourceQuota / LimitRange** to constrain runaway compute and memory usage.
- **Image policy controls** (trusted registry, signature/provenance checks if available).

Without these controls, sandbox pods may still reach internal services or external networks based on cluster defaults.

## What it supports in v1

| Feature | Kubernetes | Notes |
|---|---|---|
| `create_session()` | ✅ | Creates a pod and waits until it is running. |
| `exec()` | ✅ | Uses Kubernetes exec websocket streaming. |
| Streaming output | ✅ | Stdout/stderr stream line-by-line through `SandboxProcess`. |
| Sandbox log forwarding | ✅ | Output is forwarded into `sandbox:<session_id>` step logs. |
| `destroy()` | ✅ | Deletes the backing pod. |
| `attach(session_id)` | ❌ | Not implemented in v1. |
| `snapshot()` / `restore()` | ❌ | Not implemented in v1. |
| `upload_file` / `download_file` | ❌ | Not implemented in v1. |

## Lifecycle behavior

- `close()` closes only the local session handle; it does not delete the pod.
- `destroy()` deletes the backing pod and then closes the handle.
- If session startup fails, ZenML attempts best-effort pod cleanup before surfacing the error.

## Security notes

This flavor does not enforce a single security posture by itself. Isolation depends on your cluster controls:

- namespace and RBAC boundaries
- pod security admission settings
- network policies
- image provenance and runtime hardening

Treat `sandbox_environment` values as visible to executed code. Do not inject secrets you do not want sandbox code to read.

If you enable `automount_service_account_token=True`, code running in the sandbox can use in-pod credentials to call the Kubernetes API according to that service account's RBAC permissions.
