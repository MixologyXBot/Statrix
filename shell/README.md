<div align="center">

# 🖥️ Statrix Server Agent

**Cross-platform server telemetry agent for [Statrix](https://github.com/HellFireDevil18/Statrix).**

Lightweight agents for **Linux**, **macOS**, and **Windows** that report CPU, RAM, disk, network, and service metrics to your Statrix instance.

</div>

---

## 📊 Supported Platforms

| Platform | Script | Scheduler |
|:--------:|--------|-----------|
| 🐧 **Linux** | `shell/linux/statrix_install.sh` | `cron` or `systemd` timer |
| 🍎 **macOS** | `shell/macOS/statrix_install.sh` | `launchd` |
| 🪟 **Windows** | `shell/windows/statrix_install.ps1` | Windows Scheduled Task |

---

## 🚀 Installation

The recommended way to install the agent is through the **Statrix dashboard**:

1. 🖥️ Create a **Server Agent** monitor in the dashboard (https://{your-statrix-app-url}/edit).
2. 📋 Copy the generated install command.
3. ▶️ Run it on the target machine with elevated privileges (`sudo` / Administrator).

The command includes your endpoint URL, SID, and monitoring options — no manual configuration needed.

---

## 🔒 Security

- 🔐 **Use HTTPS** in production — agent payloads contain server metrics.
- 🔑 **Protect the SID** — it authenticates the agent to your Statrix instance. Treat it as a secret.
- 👤 **Least privilege** — run the agent with minimal permissions where possible.
- 🛡️ **Firewall** — restrict outbound traffic to your Statrix endpoint only.

---

## 🙏 Credits

The original monitoring agent concept is based on work by [HetrixTools](https://hetrixtools.com/). Adapted and maintained by Statrix contributors.

---

## 📄 License

[MIT](../LICENSE)
