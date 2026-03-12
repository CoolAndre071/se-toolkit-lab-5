# `Linux`

<h2>Table of contents</h2>

- [What is `Linux`](#what-is-linux)
- [User](#user)
- [Group](#group)
  - [The `root` user](#the-root-user)
  - [A non-root user](#a-non-root-user)
- [Permissions](#permissions)
- [`Linux` administration](#linux-administration)
  - [Change permissions](#change-permissions)
    - [`chmod`](#chmod)
    - [`chown`](#chown)
  - [Get my current user](#get-my-current-user)
  - [The `sudo` command](#the-sudo-command)
  - [Create a non-root user](#create-a-non-root-user)
  - [Inspect ports](#inspect-ports)
    - [See listening TCP ports](#see-listening-tcp-ports)
    - [Inspect a specific port](#inspect-a-specific-port)

## What is `Linux`

`Linux` is a family of [operating systems](./operating-system.md) commonly used for servers and [virtual machines](./vm.md).

There are multiple [`Linux` distros](./linux-distros.md#what-is-a-linux-distro).

## User

See [User](./operating-system.md#user).

## Group

See [Group](./operating-system.md#group).

### The `root` user

`root` is the administrator [user](#user).

### A non-root user

<!-- TODO -->

## Permissions

On [`Linux`](#what-is-linux), each [file](./file-system.md#file) and [directory](./file-system.md#directory) has [permissions](./operating-system.md#permission) that control access for three categories: the owning [user](./operating-system.md#user), the owning [group](./operating-system.md#group), and everyone else.

Each category can have three types of access:

- **Read (`r`)** — view the contents of a file or list the contents of a directory.
- **Write (`w`)** — modify a file or add/remove files in a directory.
- **Execute (`x`)** — run a file as a program or enter a directory.

Permissions are often represented as a three-digit number (e.g., `755`, `600`), where each digit encodes the read, write, and execute flags for the owner, group, and others respectively.

See:

- [Change permissions](#change-permissions).

## `Linux` administration

`Linux` administration covers system-level tasks such as managing [users](./operating-system.md#user), [groups](./operating-system.md#group), and [permissions](#permissions) on a [`Linux`](#what-is-linux) system.

### Change permissions

Changing [permissions](#permissions) controls who can read, write, or execute a file or directory, and who owns it.

Commands for changing permissions:

- [`chmod`](#chmod)
- [`chown`](#chown)

#### `chmod`

`chmod` changes the permissions of a file or directory.

To set permissions on a file or directory,

[run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

```terminal
chmod <mode> <path>
```

Common modes:

- `700` — owner can read, write, and execute; no access for group or others.
- `600` — owner can read and write; no access for group or others.
- `644` — owner can read and write; group and others can read.

#### `chown`

`chown` changes the owner and group of a file or directory.

To change the owner and group,

[run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

```terminal
chown <user>:<group> <path>
```

To change the owner and group recursively for a directory and its contents, add the `-R` flag:

```terminal
chown -R <user>:<group> <path>
```

### Get my current user

1. To get the current user,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   whoami
   ```

### The `sudo` command

`sudo` runs a command with elevated permissions.

To run a command with elevated permissions,

[run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

```terminal
sudo <command>
```

### Create a non-root user

`root` is useful for initial setup, but daily work should be done with a regular user.

For `Ubuntu`/`Debian` systems:

1. To create a new user,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   sudo adduser <username>
   ```

2. To allow the user to run administrative commands,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   sudo usermod -aG sudo <username>
   ```

3. To switch to that user,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   su - <username>
   ```

4. To verify the current user,

   [run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   whoami
   id
   ```

If you plan to log in via `SSH` as that user, copy `authorized_keys` to the new user's home and fix permissions before logging out from `root`.

### Inspect ports

Use the following commands to inspect [ports](./computer-networks.md#port) on a [host](./computer-networks.md#host).

- [See listening TCP ports](#see-listening-tcp-ports)
- [Inspect a specific port](#inspect-a-specific-port)

#### See listening TCP ports

To see all listening TCP ports,

[run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

```terminal
ss -ltn
```

#### Inspect a specific port

To inspect a specific port,

[run in the `VS Code Terminal`](./vs-code.md#run-a-command-in-the-vs-code-terminal):

```terminal
ss -ltn 'sport = :42000'
```
