package processors

import (
	"golang.org/x/crypto/ssh"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/logger"
	"github.com/DailyBurn/ratchet/util"
	"github.com/pkg/sftp"
)

// SftpWriter is an inline writer to remote sftp server
type SftpWriter struct {
	ftpFilepath string
	client      *sftp.Client
	file        *sftp.File
	params      *parameters
	initialized bool
}

type parameters struct {
	server   string
	username string
	password string
	path     string
}

// NewSftpWriter instantiates a new writer and opens a connection to the remote server creating the file referenced in the path
func NewSftpWriter(server, username, password, path string) *SftpWriter {
	return &SftpWriter{params: &parameters{server, username, password, path}, initialized: false}
}

func (w *SftpWriter) init(killChan chan error) {
	c := connect(w.params.server, w.params.username, w.params.password, killChan)

	f, e := c.Create(w.params.path)
	if e != nil {
		util.KillPipelineIfErr(e, killChan)
	}
	w.client = c
	w.file = f
	w.initialized = true
}

// connect opens an ssh connection and instantiates an sftp client based on that connection
func connect(server, username, password string, killChan chan error) *sftp.Client {
	// open ssh connection
	config := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
	}
	c, e := ssh.Dial("tcp", server, config)
	if e != nil {
		util.KillPipelineIfErr(e, killChan)
	}

	// instantiate sftp client
	sftp, e := sftp.NewClient(c)
	if e != nil {
		util.KillPipelineIfErr(e, killChan)
	}

	return sftp
}

// ProcessData writes data as is directly to the output file
func (w *SftpWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	logger.Debug("FTPWriter Process data:", string(d))
	if !w.initialized {
		w.init(killChan)
	}
	w.file.Write([]byte(d))
}

// Finish closes open references to the remote file and server
func (w *SftpWriter) Finish(outputChan chan data.JSON, killChan chan error) {
	w.file.Close()
	w.client.Close()
}

func (f *SftpWriter) String() string {
	return "SftpWriter"
}
