# rstracer-dashboard

[![CI](https://github.com/VictorMeyer77/rstracer-dashboard/actions/workflows/ci.yml/badge.svg)](https://github.com/VictorMeyer77/rstracer-dashboard/actions/workflows/ci.yml)

**A Behavior Analysis Tool for UNIX Systems**

---

## Table of Contents

1. [About the Project](#about-the-project)
2. [Features](#features)
3. [Prerequisites](#prerequisites)
4. [Installation](#installation)
5. [Usage](#usage)
6. [Configuration](#configuration)
7. [Limitations](#limitations)

---

## About the Project

`rstracer-dashboard` provides a graphical interface for [rstracer](https://github.com/VictorMeyer77/rstracer-dashboard), enabling quasi-real-time monitoring of your system's activity, including processes, open files, and network connections.

---

## Features

- Monitor command processes, open files, and network activity in real-time.
- User-friendly GUI for streamlined interaction.
- Export analysis data in Parquet format for sharing or storage.
- Docker-compatible for isolated environments.

---

## Prerequisites

Install `rstracer` and its dependencies:

```shell
cargo install --git https://github.com/VictorMeyer77/rstracer.git --tag 0.1.0
```

Additionally, ensure the following dependencies are installed:

```shell
apt-get update && apt-get install -y \
    libpcap-dev \
    git \
    build-essential \
    lsof \
    dnsutils \
    python3-pip \
    sudo
```

---

## Installation

Set up the project environment and install the required packages:

```shell
make virtualenv
source .venv/bin/activate
make install
```

---

## Usage

Run the dashboard using the command below:

```python
streamlit run rsdb.py
```

> **Note:**  
> - As the tool analyzes network activity, administrative permissions are required.  
> - If prompted for a password during execution, restart the command with the appropriate permissions to ensure reliable functionality.

---

## Configuration

The tool applies a default configuration by default. For customization, edit the [rstracer.toml](rstracer.toml) file.

---

## Limitations

1. **System Language**: The `ps` command requires the system language to be set to English for proper date parsing.  
2. **Platform Support**: The tool is only available for UNIX-based systems.
