package processors

import (
	"golang.org/x/crypto/ssh"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/logger"
	"github.com/pkg/sftp"
)

type SftpWriter struct {
	ftpFilepath string
	client      *sftp.Client
	file        *sftp.File
}

func NewSftpWriter(server, username, password, path string) *SftpWriter {
	c, e := connect(server, username, password)
	if e != nil {
		logger.Error("Error instantiating ftp writer", e)
		return nil
	}

	file, e := c.Create(path)
	if e != nil {
		logger.Error("Error instantiating ftp writer", e)
		return nil
	}

	f := SftpWriter{file: file, client: c}

	return &f
}

func connect(server, username, password string) (*sftp.Client, error) {
	// open ssh connection
	config := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
	}
	c, err := ssh.Dial("tcp", server, config)
	if err != nil {
		return nil, err
	}

	// instantiate sftp client
	sftp, err := sftp.NewClient(c)
	if err != nil {
		return nil, err
	}

	return sftp, nil
}

func (w *SftpWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	logger.Debug("FTPWriter Process data:", string(d))
	w.file.Write([]byte(d))
}

func (w *SftpWriter) Finish(outputChan chan data.JSON, killChan chan error) {
	w.file.Close()
	w.client.Close()
}

func (f *SftpWriter) String() string {
	return "SftpWriter"
}
