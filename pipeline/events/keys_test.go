package events

import (
	"testing"
)

func TestGenerateBlobEventKey(t *testing.T) {
	tests := []struct {
		name         string
		subscription string
		environment  string
		eventType    string
		blobName     string
		expected     string
	}{
		{
			name:         "simple blob name",
			subscription: "cp2",
			environment:  "D1",
			eventType:    "observed",
			blobName:     "test.log",
			expected:     "cp2:D1:observed:test.log",
		},
		{
			name:         "blob name with dashes",
			subscription: "cp2",
			environment:  "P1",
			eventType:    "line-1",
			blobName:     "20250609.zookeeper-0_default_zookeeper-c7bc5a98e73ebf8ae9e8bf3c4815a7df984c382142fa57012e42137f608015d0.gz",
			expected:     "cp2:P1:line-1:20250609.zookeeper-0_default_zookeeper-c7bc5a98e73ebf8ae9e8bf3c4815a7df984c382142fa57012e42137f608015d0.gz",
		},
		{
			name:         "kubernetes prefix removed",
			subscription: "cp2",
			environment:  "S1",
			eventType:    "closed",
			blobName:     "kubernetes/test-blob.gz",
			expected:     "cp2:S1:closed:test-blob.gz",
		},
		{
			name:         "completion event",
			subscription: "cp2",
			environment:  "P1",
			eventType:    "offset1024-completion",
			blobName:     "my-complex-blob-name.gz",
			expected:     "cp2:P1:offset1024-completion:my-complex-blob-name.gz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GenerateBlobEventKey(tt.subscription, tt.environment, tt.eventType, tt.blobName)
			if result != tt.expected {
				t.Errorf("GenerateBlobEventKey() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestParseBlobEventKey(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		expected  *BlobEventKey
		shouldErr bool
	}{
		{
			name: "simple key",
			key:  "cp2:D1:observed:test.log",
			expected: &BlobEventKey{
				Subscription: "cp2",
				Environment:  "D1",
				EventType:    "observed",
				BlobName:     "test.log",
			},
			shouldErr: false,
		},
		{
			name: "complex blob name with dashes",
			key:  "cp2:P1:line-1:20250609.zookeeper-0_default_zookeeper-c7bc5a98e73ebf8ae9e8bf3c4815a7df984c382142fa57012e42137f608015d0.gz",
			expected: &BlobEventKey{
				Subscription: "cp2",
				Environment:  "P1",
				EventType:    "line-1",
				BlobName:     "20250609.zookeeper-0_default_zookeeper-c7bc5a98e73ebf8ae9e8bf3c4815a7df984c382142fa57012e42137f608015d0.gz",
			},
			shouldErr: false,
		},
		{
			name: "completion event",
			key:  "cp2:P1:offset1024-completion:my-complex-blob-name.gz",
			expected: &BlobEventKey{
				Subscription: "cp2",
				Environment:  "P1",
				EventType:    "offset1024-completion",
				BlobName:     "my-complex-blob-name.gz",
			},
			shouldErr: false,
		},
		{
			name:      "invalid key format",
			key:       "cp2:D1:observed", // missing blob name
			expected:  nil,
			shouldErr: true,
		},
		{
			name:      "empty key",
			key:       "",
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseBlobEventKey(tt.key)

			if tt.shouldErr {
				if err == nil {
					t.Errorf("ParseBlobEventKey() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("ParseBlobEventKey() unexpected error: %v", err)
				return
			}

			if result.Subscription != tt.expected.Subscription {
				t.Errorf("Subscription = %v, want %v", result.Subscription, tt.expected.Subscription)
			}
			if result.Environment != tt.expected.Environment {
				t.Errorf("Environment = %v, want %v", result.Environment, tt.expected.Environment)
			}
			if result.EventType != tt.expected.EventType {
				t.Errorf("EventType = %v, want %v", result.EventType, tt.expected.EventType)
			}
			if result.BlobName != tt.expected.BlobName {
				t.Errorf("BlobName = %v, want %v", result.BlobName, tt.expected.BlobName)
			}
		})
	}
}

func TestBlobEventKeyMethods(t *testing.T) {
	key := &BlobEventKey{
		Subscription: "cp2",
		Environment:  "P1",
		EventType:    "line-123",
		BlobName:     "test.log",
	}

	if !key.IsLogLine() {
		t.Error("IsLogLine() should return true for line-* events")
	}

	if key.IsCompletion() {
		t.Error("IsCompletion() should return false for line-* events")
	}

	if key.IsObserved() {
		t.Error("IsObserved() should return false for line-* events")
	}

	// Test completion event
	completionKey := &BlobEventKey{
		Subscription: "cp2",
		Environment:  "P1",
		EventType:    "offset1024-completion",
		BlobName:     "test.log",
	}

	if !completionKey.IsCompletion() {
		t.Error("IsCompletion() should return true for *-completion events")
	}

	if completionKey.IsLogLine() {
		t.Error("IsLogLine() should return false for completion events")
	}
}

func TestRoundTrip(t *testing.T) {
	original := "cp2:P1:closed:20250609.zookeeper-0_default_zookeeper-c7bc5a98e73ebf8ae9e8bf3c4815a7df984c382142fa57012e42137f608015d0.gz"

	parsed, err := ParseBlobEventKey(original)
	if err != nil {
		t.Fatalf("ParseBlobEventKey() error: %v", err)
	}

	regenerated := parsed.String()
	if regenerated != original {
		t.Errorf("Round trip failed: original=%v, regenerated=%v", original, regenerated)
	}
}
