// Package util has various helper functions used by components of ratchet.
package util

// KillPipelineIfErr is an error-checking helper.
func KillPipelineIfErr(err error, killChan chan error) {
	if err != nil {
		killChan <- err
	}
}
