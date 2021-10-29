package entangler

import (
	"fmt"
	"testing"

	"github.com/relab/snarl-mw21/utils"
	"github.com/stretchr/testify/assert"
)

func TestRepair(t *testing.T) {
	// Enable log printing.
	utils.Enable_LogPrint()

	// *****  START: Positive tests (Should pass.)  ****** //
	testSetups := make([]*testsetup, 0)
	t.Run("RootDataFailure", func(t *testing.T) {
		r_RootDataFailure(&testSetups)
	})
	t.Run("Middleware32Failure", func(t *testing.T) {
		r_Middleware32Failure(&testSetups)
	})
	t.Run("Middleware64Failure", func(t *testing.T) {
		r_Middleware64Failure(&testSetups)
	})
	t.Run("SingleDataFailure", func(t *testing.T) {
		r_SingleDataFailure(&testSetups)
	})
	t.Run("SingleParityFailure", func(t *testing.T) {
		r_SingleParityFailure(&testSetups)
	})
	t.Run("SingleDataReplaceFailure", func(t *testing.T) {
		r_SingleDataReplaceFailure(&testSetups)
	})
	t.Run("ConsecutiveDataFailure", func(t *testing.T) {
		r_ConsecutiveDataFailure(&testSetups)
	})
	t.Run("ConsecutiveParityFailure", func(t *testing.T) {
		r_ConsecutiveParityFailure(&testSetups)
	})
	t.Run("BigLatticeDataParityFailure", func(t *testing.T) {
		r_BigLatticeDataParityFailure(&testSetups)
	})
	t.Run("BigLatticeDataParityFailureTwo", func(t *testing.T) {
		r_BigLatticeDataParityFailureTwo(&testSetups)
	})
	t.Run("BigLatticeDataParityFailureThree", func(t *testing.T) {
		r_BigLatticeDataParityFailureThree(&testSetups)
	})
	t.Run("BigLatticeDataParityFailureFour", func(t *testing.T) {
		r_BigLatticeDataParityFailureFour(&testSetups)
	})
	t.Run("FirstColumnFailure", func(t *testing.T) {
		r_FirstColumnFailure(&testSetups)
	})
	t.Run("RecursiveRepair", func(t *testing.T) {
		r_RecursiveRepair(&testSetups)
	})
	t.Run("ManyDataParityFailureSpecialRepair", func(t *testing.T) {
		r_ManyDataParityFailureSpecialRepair(&testSetups)
	})
	t.Run("ManyDataParityMultiBranchFailure", func(t *testing.T) {
		r_ManyDataParityMultiBranchFailure(&testSetups)
	})
	t.Run("TreeNodeAndLocalRepairLost", func(t *testing.T) {
		r_TreeNodeAndLocalRepairLost(&testSetups)
	})
	t.Run("TreeNodeAndLongPath", func(t *testing.T) {
		r_TreeNodeAndLongPath(&testSetups)
		r_TreeNodeAndLongPathReverse(&testSetups)
	})
	t.Run("TwoTreeNodesAndLocalRepairLost", func(t *testing.T) {
		r_TwoTreeNodesAndLocalRepairLost(&testSetups)
		r_TwoTreeNodesAndLocalRepairLostReverse(&testSetups)
	})
	t.Run("TwoTreeNodesBigLoss", func(t *testing.T) {
		r_TwoTreeNodesAndLocalRepairLostBoth(&testSetups)
	})
	t.Run("Specific10PercentFailureOne", func(t *testing.T) {
		r_Specific10PercentFailureOne(&testSetups)
	})
	t.Run("Specific20PercentFailureTwo", func(t *testing.T) {
		r_Specific20PercentFailureTwo(&testSetups)
	})
	t.Run("Specific40PercentFailureOne", func(t *testing.T) {
		r_Specific40PercentFailureOne(&testSetups)
	})
	t.Run("Specific40PercentFailureTwo", func(t *testing.T) {
		r_Specific40PercentFailureTwo(&testSetups)
	})
	t.Run("Specific40PercentFailureThree", func(t *testing.T) {
		r_Specific40PercentFailureThree(&testSetups)
	})
	t.Run("Specific40PercentFailureFour", func(t *testing.T) {
		r_Specific40PercentFailureFour(&testSetups)
	})
	t.Run("Specific40PercentFailureFive", func(t *testing.T) {
		r_Specific40PercentFailureFive(&testSetups)
	})
	t.Run("Specific40PercentFailureSix", func(t *testing.T) {
		r_Specific40PercentFailureSix(&testSetups)
	})
	t.Run("Specific50PercentFailureOne", func(t *testing.T) {
		r_Specific50PercentFailureOne(&testSetups)
	})
	t.Run("Specific50PercentFailureTwo", func(t *testing.T) {
		r_Specific50PercentFailureTwo(&testSetups)
	})
	t.Run("RepairPendingInfiniteLoop", func(t *testing.T) {
		r_RepairPendingInfiniteLoop(&testSetups)
	})

	testFailures := RunTests(testSetups)
	var haveFailures bool = false
	for i := 0; i < len(testSetups); i++ {
		if testFailures[i] != "" && !testSetups[i].ShouldFail {
			haveFailures = true
			fmt.Printf("---------------------------------------------\nError in test: %v\n%v\n---------------------------------------------\n",
				testSetups[i].Description, testFailures[i])
		} else if testFailures[i] == "" && testSetups[i].ShouldFail {
			haveFailures = true
			fmt.Printf("---------------------------------------------\nError in test: %v\n%v\n---------------------------------------------\n",
				testSetups[i].Description, "Expected the test to fail!")
		}
	}
	assert.False(t, haveFailures)
}

func makeRange(start, stop, interval int) (theRange []int) {
	for i := start; i < stop; i += interval {
		theRange = append(theRange, i)
	}
	return
}
