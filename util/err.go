package util

// KillPipelineIfErr is an error-checking helper.
func KillPipelineIfErr(err error, killChan chan error) {
	if err != nil {
		killChan <- err
	}
}
