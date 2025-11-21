package datasetUtils

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

type mockCount struct {
	origDatablocks int
	attachments    int
	datasets       int
	datablocks     []int
}

func TestRemoveFromCatalog_AllCases(t *testing.T) {
	tests := []struct {
		name      string
		mockCount mockCount
		expected  []string
	}{
		{
			name: "Delete none immediately",
			mockCount: mockCount{
				origDatablocks: 0,
				attachments:    0,
				datasets:       0,
				datablocks:     []int{0},
			},
			expected: []string{},
		},
		{
			name: "Delete 1 origdatablocks immediately",
			mockCount: mockCount{
				origDatablocks: 1,
				attachments:    0,
				datasets:       0,
				datablocks:     []int{0},
			},
			expected: []string{
				"/Datasets/dataset1/origdatablocks",
			},
		},
		{
			name: "Delete 1 attachments immediately",
			mockCount: mockCount{
				origDatablocks: 0,
				attachments:    1,
				datasets:       0,
				datablocks:     []int{0},
			},
			expected: []string{
				"/Datasets/dataset1/attachments",
			},
		},
		{
			name: "Delete 1 dataset immediately",
			mockCount: mockCount{
				origDatablocks: 0,
				attachments:    0,
				datasets:       1,
				datablocks:     []int{0},
			},
			expected: []string{
				"/Datasets/dataset1",
			},
		},
		{
			name: "Delete all immediately",
			mockCount: mockCount{
				origDatablocks: 1,
				attachments:    1,
				datasets:       1,
				datablocks:     []int{0},
			},
			expected: []string{
				"/Datasets/dataset1/origdatablocks",
				"/Datasets/dataset1/attachments",
				"/Datasets/dataset1",
			},
		},
		{
			name: "Delete all after waiting twice datablocks deletion",
			mockCount: mockCount{
				origDatablocks: 1,
				attachments:    1,
				datasets:       1,
				datablocks:     []int{1, 0},
			},
			expected: []string{
				"/Datasets/dataset1/origdatablocks",
				"/Datasets/dataset1/attachments",
				"/Datasets/dataset1",
			},
		},
	}

	user := map[string]string{
		"mail":        "test@example.com",
		"username":    "testuser",
		"accessToken": "testtoken",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calledDeletes := []string{}

			dbCounts := tt.mockCount.datablocks
			getDatablocksCount := func() int {
				if len(dbCounts) == 0 {
					return 0
				}
				count := dbCounts[0]
				dbCounts = dbCounts[1:]
				return count
			}
			dbCalls := 0

			client := &http.Client{
				Transport: &MockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						if req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/count") {
							var count int
							switch {
							case strings.Contains(req.URL.Path, "origdatablocks"):
								count = tt.mockCount.origDatablocks
							case strings.Contains(req.URL.Path, "attachments"):
								count = tt.mockCount.attachments
							case strings.Contains(req.URL.Path, "datablocks"):
								count = getDatablocksCount()
								dbCalls++
							case strings.Contains(req.URL.Path, "Datasets"):
								count = tt.mockCount.datasets
							default:
								count = 0
							}

							body := []byte(`{"count":` + strconv.Itoa(count) + `}`)
							return &http.Response{
								StatusCode: 200,
								Body:       io.NopCloser(bytes.NewBuffer(body)),
							}, nil
						}

						if req.Method == http.MethodDelete {
							calledDeletes = append(calledDeletes, req.URL.Path)
							return &http.Response{
								StatusCode: 200,
								Body:       io.NopCloser(bytes.NewBufferString(`{}`)),
							}, nil
						}

						return &http.Response{
							StatusCode: 400,
							Body:       io.NopCloser(bytes.NewBufferString(`{}`)),
						}, nil
					},
				},
			}

			RemoveFromCatalog(client, "http://mockserver", "dataset1", user, true, 0)

			if len(calledDeletes) != len(tt.expected) {
				t.Errorf("Expected %d DELETE calls, got %d: %v", len(tt.expected), len(calledDeletes), calledDeletes)
			}
			for i, endpoint := range tt.expected {
				if calledDeletes[i] != endpoint {
					t.Errorf("Expected DELETE to %s, got %s", endpoint, calledDeletes[i])
				}
			}

			if dbCalls != len(tt.mockCount.datablocks) {
				t.Errorf("Expected %d GET /datablocks calls, got %d", len(tt.mockCount.datablocks), dbCalls)
			}
		})
	}
}
