package ratchet

import "fmt"

// PipelineLayout holds a series of PipelineStage instances.
type PipelineLayout struct {
	stages []*PipelineStage
}

// NewPipelineLayout creates and validates a new PipelineLayout instance which
// can be used to create a "branching" Pipeline. A PipelineLayout consists of
// a series of PipelineStages, where each PipelineStage consists of one or more
// DataProcessors. See the ratchet package documentation for code examples and diagrams.
//
// This function will return an error if the given layout is invalid.
// A valid layout meets these conditions:
// 	1) DataProcessors in the final PipelineStage must NOT have outputs set.
// 	2) DataProcessors in a non-final stage MUST have outputs set.
// 	3) Outputs must point to a DataProcessor in the next immediate stage.
// 	4) A DataProcessor must be pointed to by one of the previous Outputs (unless it is in the first PipelineStage).
func NewPipelineLayout(stages ...*PipelineStage) (*PipelineLayout, error) {
	l := &PipelineLayout{stages}
	if err := l.validate(); err != nil {
		return nil, err
	}
	return l, nil
}

// validate returns an error or nil
// See the validation rules defined in NewPipelineLayout.
func (l *PipelineLayout) validate() error {
	var stage *PipelineStage
	for stageNum := range l.stages {
		stage = l.stages[stageNum]
		var dp *dataProcessor
		for j := range stage.processors {
			dp = stage.processors[j]
			// 1) final stages must NOT have outputs set
			// 2) non-final stages must HAVE outputs set
			if stageNum == len(l.stages)-1 && dp.outputs != nil {
				return fmt.Errorf("DataProcessor (%v) must have Outputs set in final PipelineStage", dp)
			} else if stageNum != len(l.stages)-1 && dp.outputs == nil {
				return fmt.Errorf("DataProcessor (%v) must have Outputs set in non-final PipelineStage #%d", dp, stageNum+1)
			}
			// 3) outputs must point to a DataProcessor in the next immediate stage
			if stageNum < len(l.stages)-1 {
				nextStage := l.stages[stageNum+1]
				for k := range dp.outputs {
					if !nextStage.hasProcessor(dp.outputs[k]) {
						return fmt.Errorf("DataProcessor (%v) Outputs must point to DataProcessor in the next PipelineStage #%d", dp, stageNum+2)
					}
				}
			}
			// 4) a non-starting DataProcessor must be pointed to by one of the previous outputs
			if stageNum > 0 {
				prevStage := l.stages[stageNum-1]
				if !prevStage.hasOutput(dp.DataProcessor) {
					return fmt.Errorf("DataProcessor (%v) is not pointed to by any output in the previous PipelineStage #%d", dp, stageNum)
				}
			}
		}
	}
	return nil
}
