package datasetUtils

import "testing"

func TestConvertToTransferType(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    TransferType
		wantErr bool
	}{
		{name: "ssh", input: "ssh", want: Ssh},
		{name: "globus", input: "globus", want: Globus},
		{name: "s3", input: "s3", want: S3},
		{name: "invalid", input: "lolrandom", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertToTransferType(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected an error, got: %v", got)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("ConvertToTransferType(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
