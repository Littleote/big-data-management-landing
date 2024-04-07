import paramiko


SERVICES = {
    "HDFS": {
        "start": "~/BDM_Software/hadoop/sbin/start-dfs.sh",
        "stop": "~/BDM_Software/hadoop/sbin/stop-dfs.sh",
    },
    "MongoDB": {
        "start": "~/BDM_Software/mongodb/bin/mongod --dbpath=/home/bdm/db --bind_ip_all > /dev/null 2>&1 &",
        "stop": "~/BDM_Software/mongodb/bin/mongod --dbpath=/home/bdm/db --shutdown",
    },
}


class Service:
    def __init__(self, host: str, username: str | None, password: str | None):
        self.host = host
        self.username = username
        self.password = password
        self.connect = not (username is None or password is None)
        self.running: list[str] = []

    def __enter__(self) -> paramiko.SSHClient | None:
        if not self.connect:
            return None
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            self.ssh.connect(self.host, username=self.username, password=self.password)
            print("SSH connection established successfully.")

            for service, commands in SERVICES.items():
                if service in self.running:
                    continue

                print(f"Starting {service}")
                # Execute the command to start the service
                _, stdout, stderr = self.ssh.exec_command(commands["start"])

                # Read and print output
                print("Output:")
                for line in stdout:
                    print(line.strip())

                # Check for any errors
                if stderr.channel.recv_exit_status() == 0:
                    self.running.append(service)
                else:
                    print("Error:", stderr.read().decode())

        except paramiko.AuthenticationException:
            print("Authentication failed, please verify your credentials")
        except paramiko.SSHException as ssh_ex:
            print("Unable to establish SSH connection:", ssh_ex)
        finally:
            print("Done")
            return self.ssh

    def __exit__(self, type, value, traceback):
        if not self.connect:
            return
        for service, commands in SERVICES.items():
            if service not in self.running:
                continue

            print(f"Stopping {service}")
            # Execute the command to stop the service
            _, stdout, stderr = self.ssh.exec_command(commands["stop"])

            # Read and print output
            print("Output:")
            for line in stdout:
                print(line.strip())

            # Check for any errors
            if stderr.channel.recv_exit_status() == 0:
                self.running.remove(service)
            else:
                print("Error:", stderr.read().decode())

        if len(self.running) != 0:
            print("Failed to shutdown the following services:")
            for service in self.running:
                print(service)

        self.ssh.close()
