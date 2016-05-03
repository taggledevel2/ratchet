package util

import (
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func SftpClient(server string, username string, authMethd []ssh.AuthMethod) (*sftp.Client, error) {
	var client *sftp.Client

	ssh.ClientConfig
	config := &ssh.ClientConfig{
		User: username,
		Auth: authMethd,
	}

	conn, err := ssh.Dial("tcp", server, config)
	if err != nil {
		return client, err
	}

	return sftp.NewClient(conn)
}
