package entangler_test

import (
	"testing"

	. "github.com/relab/snarl-mw21/entangler"
)

func TestBlockStatus(t *testing.T) {
	tests := []struct {
		ds DownloadStatus
		rs RepairStatus
		bs BlockStatus
	}{
		{bs: 0b0000, ds: NoDownload, rs: NoRepair},
		{bs: 0b0001, ds: DownloadPending, rs: NoRepair},
		{bs: 0b0010, ds: DownloadSuccess, rs: NoRepair},
		{bs: 0b0011, ds: DownloadFailed, rs: NoRepair},
		{bs: 0b0100, ds: NoDownload, rs: RepairPending},
		{bs: 0b0101, ds: DownloadPending, rs: RepairPending},
		{bs: 0b0110, ds: DownloadSuccess, rs: RepairPending},
		{bs: 0b0111, ds: DownloadFailed, rs: RepairPending},
		{bs: 0b1000, ds: NoDownload, rs: RepairSuccess},
		{bs: 0b1001, ds: DownloadPending, rs: RepairSuccess},
		{bs: 0b1010, ds: DownloadSuccess, rs: RepairSuccess},
		{bs: 0b1011, ds: DownloadFailed, rs: RepairSuccess},
		{bs: 0b1100, ds: NoDownload, rs: RepairFailed},
		{bs: 0b1101, ds: DownloadPending, rs: RepairFailed},
		{bs: 0b1110, ds: DownloadSuccess, rs: RepairFailed},
		{bs: 0b1111, ds: DownloadFailed, rs: RepairFailed},
	}
	for _, test := range tests {
		bs := Set(test.ds, test.rs)
		if bs != test.bs {
			t.Errorf("Set(%d, %d) = %d, expected %d", test.ds, test.rs, bs, test.bs)
		}
	}
}

func TestBlockStatusHasData(t *testing.T) {
	tests := []struct {
		ds      DownloadStatus
		rs      RepairStatus
		hasData bool
	}{
		{ds: NoDownload, rs: NoRepair, hasData: false},
		{ds: DownloadPending, rs: NoRepair, hasData: false},
		{ds: DownloadSuccess, rs: NoRepair, hasData: true},
		{ds: DownloadFailed, rs: NoRepair, hasData: false},
		{ds: NoDownload, rs: RepairPending, hasData: false},
		{ds: NoDownload, rs: RepairSuccess, hasData: true},
		{ds: NoDownload, rs: RepairFailed, hasData: false},
		{ds: DownloadPending, rs: RepairPending, hasData: false},
		{ds: DownloadPending, rs: RepairSuccess, hasData: true},
		{ds: DownloadPending, rs: RepairFailed, hasData: false},
		{ds: DownloadSuccess, rs: RepairPending, hasData: true},
		{ds: DownloadSuccess, rs: RepairSuccess, hasData: true},
		{ds: DownloadSuccess, rs: RepairFailed, hasData: true},
		{ds: DownloadFailed, rs: RepairPending, hasData: false},
		{ds: DownloadFailed, rs: RepairSuccess, hasData: true},
		{ds: DownloadFailed, rs: RepairFailed, hasData: false},
	}
	for _, test := range tests {
		bs := Set(test.ds, test.rs)
		got := bs.HasData()
		if got != test.hasData {
			t.Errorf("HasData(%d, %d) = %t, expected %t", test.ds, test.rs, got, test.hasData)
		}
	}
}
