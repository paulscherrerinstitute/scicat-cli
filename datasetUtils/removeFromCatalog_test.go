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
				"/Datasets/dataset%2F1/origdatablocks",
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
				"/Datasets/dataset%2F1/attachments",
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
				"/Datasets/dataset%2F1",
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
				"/Datasets/dataset%2F1/origdatablocks",
				"/Datasets/dataset%2F1/attachments",
				"/Datasets/dataset%2F1",
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
				"/Datasets/dataset%2F1/origdatablocks",
				"/Datasets/dataset%2F1/attachments",
				"/Datasets/dataset%2F1",
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
								expected := "/Datasets/dataset%2F1/origdatablocks/count"
								if req.URL.RawPath != expected {
									t.Errorf("GET /count path mismatch for origdatablocks: got %s, expected %s", req.URL.RawPath, expected)
								}
							case strings.Contains(req.URL.Path, "attachments"):
								count = tt.mockCount.attachments
								expected := "/Datasets/dataset%2F1/attachments/count"
								if req.URL.RawPath != expected {
									t.Errorf("GET /count path mismatch for attachments: got %s, expected %s", req.URL.RawPath, expected)
								}
							case strings.Contains(req.URL.Path, "datablocks"):
								count = getDatablocksCount()
								dbCalls++
								expected := "/Datasets/dataset%2F1/datablocks/count"
								if req.URL.RawPath != expected {
									t.Errorf("GET /count path mismatch for datablocks: got %s, expected %s", req.URL.RawPath, expected)
								}
							case strings.Contains(req.URL.Path, "Datasets"):
								count = tt.mockCount.datasets
								expected := "/Datasets/count"
								if req.URL.Path != expected {
									t.Errorf("GET /count path mismatch for datasets: got %s, expected %s", req.URL.Path, expected)
								}
								expectedQuery := "filter=%7B%22where%22%3A%7B%22pid%22%3A%22dataset%2F1%22%7D%7D"
								if req.URL.RawQuery != expectedQuery {
									t.Errorf("GET /count path mismatch for datasets: got %s, expected %s", req.URL.RawQuery, expectedQuery)
								}
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
							calledDeletes = append(calledDeletes, req.URL.RawPath)
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

			RemoveFromCatalog(client, "http://mockserver", "dataset/1", user, true, 0)

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
