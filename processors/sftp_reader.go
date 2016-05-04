package processors

import (
	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/util"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SftpReader struct {
	IoReader      // embeds IoReader
	parameters    *util.SftpParameters
	client        *sftp.Client
	DeleteObjects bool
	Walk          bool
	initialized   bool
}

// NewSftpReader reads a single object at a given path, or walks through the
// directory specified by the path (SftpReader.Walk must be set to true)
func NewSftpReader(server string, username string, path string, authMethods ...ssh.AuthMethod) *SftpReader {
	r := SftpReader{parameters: &util.SftpParameters{server, username, path, authMethods}, initialized: false}
	r.IoReader.LineByLine = true
	return &r
}

func (r *SftpReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	r.ensureInitialized(killChan)
	if r.Walk {
		r.walk(outputChan, killChan)
	} else {
		r.sendFile(r.parameters.Path, outputChan, killChan)
	}
}

func (r *SftpReader) Finish(outputChan chan data.JSON, killChan chan error) {}

func (r *SftpReader) String() string {
	return "SftpReader"
}

func (r *SftpReader) ensureInitialized(killChan chan error) {
	if r.initialized {
		return
	}

	client, err := util.SftpClient(r.parameters.Server, r.parameters.Username, r.parameters.AuthMethods)
	util.KillPipelineIfErr(err, killChan)

	r.client = client
	r.initialized = true
}

func (r *SftpReader) walk(outputChan chan data.JSON, killChan chan error) {
	walker := r.client.Walk(r.parameters.Path)
	for walker.Step() {
		util.KillPipelineIfErr(walker.Err(), killChan)
		if !walker.Stat().IsDir() {
			r.sendFile(walker.Path(), outputChan, killChan)
		}
	}
}

func (r *SftpReader) sendFile(path string, outputChan chan data.JSON, killChan chan error) {
	r.ensureInitialized(killChan)

	file, err := r.client.Open(path)
	defer file.Close()

	util.KillPipelineIfErr(err, killChan)

	r.IoReader.Reader = file
	r.IoReader.ProcessData(nil, outputChan, killChan)

	if r.DeleteObjects {
		err = r.client.Remove(path)
		util.KillPipelineIfErr(err, killChan)
	}
}
