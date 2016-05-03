package util

import (
	"io/ioutil"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// Set up and return the sftp client
func SftpClient(server string, username string, authMethod []ssh.AuthMethod) (client *sftp.Client, err error) {
	ssh.ClientConfig
	config := &ssh.ClientConfig{
		User: username,
		Auth: authMethod,
	}

	conn, err := ssh.Dial("tcp", server, config)
	if err != nil {
		return
	}

	client = sftp.NewClient(conn)
	return
}

// Generate an ssh.AuthMethod given the path of a private key
func SftpKeyAuth(privateKeyPath string) (auth ssh.AuthMethod, err error) {
	privateKey, err := ioutil.ReadFile(privateKeyPath)
	if err != nil {
		return
	}

	signer, err := ssh.ParsePrivateKey([]byte(privateKey))
	if err != nil {
		return
	}

	auth = ssh.PublicKeys(signer)

	return
}
