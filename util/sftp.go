package util

import (
	"io/ioutil"
	"path/filepath"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SftpParameters struct {
	Server      string
	Username    string
	Path        string
	AuthMethods []ssh.AuthMethod
}

type SftpPath struct {
	Path string `json:"path,omitempty"`
}

func (t SftpPath) FileName() string {
	return filepath.Base(t.Path)
}

// Set up and return the sftp client
func SftpClient(server string, username string, authMethod []ssh.AuthMethod, opts ...func(*sftp.Client) error) (*sftp.Client, error) {
	var client *sftp.Client

	config := &ssh.ClientConfig{
		User: username,
		Auth: authMethod,
	}

	conn, err := ssh.Dial("tcp", server, config)
	if err != nil {
		return client, err
	}

	return sftp.NewClient(conn, opts...)
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
