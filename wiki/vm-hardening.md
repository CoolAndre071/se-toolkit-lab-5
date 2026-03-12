# VM hardening

<h2>Table of contents</h2>

- [What is VM hardening](#what-is-vm-hardening)
- [Hardening steps](#hardening-steps)
  - [Create a non-root user](#create-a-non-root-user)
  - [Set up SSH key authentication for the new user](#set-up-ssh-key-authentication-for-the-new-user)
  - [Configure `ufw` firewall](#configure-ufw-firewall)
  - [Configure `fail2ban`](#configure-fail2ban)
  - [Harden `SSH` config](#harden-ssh-config)
  - [Restart `sshd`](#restart-sshd)

## What is VM hardening

VM hardening is the process of securing a [virtual machine](./vm.md#what-is-a-vm) by reducing its attack surface.

Docs:

- [CIS Benchmarks](https://www.cisecurity.org/cis-benchmarks)

## Hardening steps

<!-- no toc -->
1. [Create a non-root user](#create-a-non-root-user)
2. [Set up SSH key authentication for the new user](#set-up-ssh-key-authentication-for-the-new-user)
3. [Configure `ufw` firewall](#configure-ufw-firewall)
4. [Configure `fail2ban`](#configure-fail2ban)
5. [Harden `SSH` config](#harden-ssh-config)
6. [Restart `sshd`](#restart-sshd)

### Create a non-root user

1. To connect to the VM as the [`root` user](./linux.md#the-root-user),

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   ssh root@<your-vm-ip-address>
   ```

   Replace [`<your-vm-ip-address>`](./vm.md#your-vm-ip-address).

2. Come up with a [username](./operating-system.md#username).

3. To create a new user,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   adduser <username>
   ```

   Replace [`<username>`](./operating-system.md#username-placeholder) with the username.

4. To add the user to the `sudo` group,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   usermod -aG sudo <username>
   ```

### Set up SSH key authentication for the new user

1. To create the `.ssh` directory for the new user,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   mkdir -p /home/<username>/.ssh
   ```

2. To copy the authorized keys from the `root` user,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   cp /root/.ssh/authorized_keys /home/<username>/.ssh/
   ```

3. To set the correct ownership on the `.ssh` directory,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   chown -R <username>:<username> /home/<username>/.ssh
   ```

4. To set the correct permissions on the `.ssh` directory,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   chmod 700 /home/<username>/.ssh
   ```

5. To set the correct permissions on the `authorized_keys` file,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   chmod 600 /home/<username>/.ssh/authorized_keys
   ```

6. To verify you can [`SSH`](./ssh.md#what-is-ssh) as the new user,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   ssh <username>@<your-vm-ip-address>
   ```

   Replace [`<your-vm-ip-address>`](./vm.md#your-vm-ip-address).

7. Confirm the connection did not prompt for a password. If it did, repeat step 3.

8. Disconnect from the root session and use the non-root user for all remaining steps.

### Configure `ufw` firewall

`ufw` (`Uncomplicated Firewall`) is a simple firewall for [`Linux`](./linux.md#what-is-linux). By default, `ufw` denies all incoming traffic. The steps below create exceptions for the ports your VM needs.

1. Find the [`<caddy-port>`](./caddy.md#caddy-port).

2. To allow [`SSH`](./ssh.md#what-is-ssh),

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   sudo ufw allow 22
   ```

   > 🟪 **Important**
   > Always allow `SSH` (port 22) before enabling `ufw`. Otherwise, you will lock yourself out of the VM.

3. To allow the application port,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   sudo ufw allow <caddy-port>
   ```

4. To enable the firewall,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   sudo ufw enable
   ```

5. To check the status,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   sudo ufw status
   ```

### Configure `fail2ban`

`fail2ban` blocks IP addresses that make too many failed login attempts. Even after password authentication is disabled, `fail2ban` remains useful: it rate-limits repeated [`SSH`](./ssh.md#what-is-ssh) connection attempts and can be extended to protect other services.

1. To update the package list,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   sudo apt update
   ```

2. To install `fail2ban`,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   sudo apt install -y fail2ban
   ```

3. To enable the service,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   sudo systemctl enable fail2ban
   ```

4. To start the service,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   sudo systemctl start fail2ban
   ```

5. To check the status,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   sudo systemctl status fail2ban
   ```

### Harden `SSH` config

1. To open the [`SSH`](./ssh.md#what-is-ssh) config,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   sudo nano /etc/ssh/sshd_config
   ```

2. Find the line `PermitRootLogin` and set it to:

   ```text
   PermitRootLogin no
   ```

3. Find the line `PasswordAuthentication` and set it to:

   ```text
   PasswordAuthentication no
   ```

4. Save and exit (`Ctrl+O`, `Enter`, `Ctrl+X`).

> [!IMPORTANT]
> Make sure you can `SSH` as a non-root user before disabling root login.

> [!IMPORTANT]
> Make sure your `SSH` key is set up before disabling password authentication.

### Restart `sshd`

After changing the [`SSH`](./ssh.md#what-is-ssh) config, restart the `SSH` service.

1. To validate the config,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   sudo sshd -t
   ```

   If the command prints no output, the config is valid. If it prints errors, fix them in `/etc/ssh/sshd_config` before continuing.

2. To restart the service,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   sudo systemctl restart sshd
   ```

3. To verify you can still connect,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   ssh <username>@<your-vm-ip-address>
   ```

   Replace [`<your-vm-ip-address>`](./vm.md#your-vm-ip-address).

> [!IMPORTANT]
> Keep your current `SSH` session open until you confirm the new connection works. If the new connection fails, use the existing session to fix the config.
