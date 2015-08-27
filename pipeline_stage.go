package ratchet

// PipelineStage holds one or more DataProcessor instances.
type PipelineStage struct {
	processors []*dataProcessor
}

// NewPipelineStage creates a PipelineStage instance given a series
// of dataProcessors. dataProcessor (lower-case d) is a private wrapper around
// an object implementing the public DataProcessor interface. The
// syntax used to create PipelineLayouts abstracts this type
// away from your implementing code. For example:
//
//     layout, err := ratchet.NewPipelineLayout(
//             ratchet.NewPipelineStage(
//                      ratchet.Do(aDataProcessor).Outputs(anotherDataProcessor),
//                      // ...
//             ),
//             // ...
//     )
//
// Notice how the ratchet.Do() and Outputs() functions allow you to insert
// DataProcessor instances into your PipelineStages without having to
// worry about the internal dataProcessor type or how any of the
// channel management works behind the scenes.
//
// See the ratchet package documentation for more code examples.
func NewPipelineStage(processors ...*dataProcessor) *PipelineStage {
	return &PipelineStage{processors}
}

func (s *PipelineStage) hasProcessor(p DataProcessor) bool {
	for i := range s.processors {
		if s.processors[i].DataProcessor == p {
			return true
		}
	}
	return false
}

func (s *PipelineStage) hasOutput(p DataProcessor) bool {
	for i := range s.processors {
		for j := range s.processors[i].outputs {
			if s.processors[i].outputs[j] == p {
				return true
			}
		}
	}
	return false
}
