package everscribe

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew_RetainsTrimmedCredentials(t *testing.T) {
	t.Parallel()
	c, err := New("  proj_123  ", "  evs_secret  ")
	require.NoError(t, err)
	require.Equal(t, "proj_123", c.projectID)
	require.Equal(t, "evs_secret", c.apiKey)
}

func TestNew_RejectsEmptyOrWhitespaceCredentials(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		projectID string
		apiKey    string
		contains  string
	}{
		{"empty projectID", "", "evs_secret", "projectID is empty"},
		{"whitespace projectID", "   ", "evs_secret", "projectID is empty"},
		{"empty apiKey", "proj_123", "", "apiKey is empty"},
		{"whitespace apiKey", "proj_123", "\t\n ", "apiKey is empty"},
		{"both empty", "", "", "projectID is empty"}, // projectID checked first
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, err := New(tc.projectID, tc.apiKey)
			require.Error(t, err)
			require.Nil(t, c)
			require.Contains(t, err.Error(), tc.contains)
		})
	}
}

func TestNewRecorder_ConstructsBufferedRecorder(t *testing.T) {
	t.Parallel()
	c, err := New("proj_123", "evs_secret")
	require.NoError(t, err)
	rec := c.NewRecorder()
	require.NotNil(t, rec)
	defer rec.Close()
}

func TestNewMinter_ConstructsMinterClient(t *testing.T) {
	t.Parallel()
	c, err := New("proj_123", "evs_secret")
	require.NoError(t, err)
	m := c.NewMinter()
	require.NotNil(t, m)
}

func TestNewFromEnv(t *testing.T) {
	// No t.Parallel here or in the subtests: t.Setenv panics in a
	// parallel test.
	for _, tc := range []struct {
		name       string
		projectID  string
		apiKey     string
		wantErr    string // substring of the error; empty means success
		wantProjID string
		wantAPIKey string
	}{
		{"success", "proj_env", "evs_env_secret", "", "proj_env", "evs_env_secret"},
		{"trims surrounding whitespace", "  proj_env  ", "\tevs_env_secret\n", "", "proj_env", "evs_env_secret"},
		{"missing project id", "", "evs_env_secret", envProjectID, "", ""},
		{"whitespace-only project id", "   ", "evs_env_secret", envProjectID, "", ""},
		{"missing api key", "proj_env", "", envAPIKey, "", ""},
		{"whitespace-only api key", "proj_env", "  ", envAPIKey, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envProjectID, tc.projectID)
			t.Setenv(envAPIKey, tc.apiKey)

			c, err := NewFromEnv()
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Nil(t, c)
				require.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantProjID, c.projectID)
			require.Equal(t, tc.wantAPIKey, c.apiKey)
		})
	}
}
