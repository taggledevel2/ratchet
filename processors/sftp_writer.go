package processors

import (
	"golang.org/x/crypto/ssh"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/logger"
	"github.com/dailyburn/ratchet/util"
	"github.com/pkg/sftp"
)

// SftpWriter is an inline writer to remote sftp server
type SftpWriter struct {
	client      *sftp.Client
	file        *sftp.File
	parameters  *util.SftpParameters
	initialized bool
}

// NewSftpWriter instantiates a new sftp writer, a connection to the remote server is delayed until data is recv'd by the writer
func NewSftpWriter(server string, username string, path string, authMethods ...ssh.AuthMethod) *SftpWriter {
	return &SftpWriter{parameters: &util.SftpParameters{server, username, path, authMethods}, initialized: false}
}

// ProcessData writes data as is directly to the output file
func (w *SftpWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	logger.Debug("SftpWriter Process data:", string(d))
	w.ensureInitialized(killChan)
	_, e := w.file.Write([]byte(d))
	util.KillPipelineIfErr(e, killChan)
}

// Finish closes open references to the remote file and server
func (w *SftpWriter) Finish(outputChan chan data.JSON, killChan chan error) {
	w.file.Close()
	w.client.Close()
}

func (f *SftpWriter) String() string {
	return "SftpWriter"
}

// ensureInitialized calls connect and then creates the output file on the sftp server at the specified path
func (w *SftpWriter) ensureInitialized(killChan chan error) {
	if w.initialized {
		return
	}

	client, err := util.SftpClient(w.parameters.Server, w.parameters.Username, w.parameters.AuthMethods)
	util.KillPipelineIfErr(err, killChan)

	logger.Info("Path", w.parameters.Path)

	file, err := client.Create(w.parameters.Path)
	util.KillPipelineIfErr(err, killChan)

	w.client = client
	w.file = file
	w.initialized = true
}
