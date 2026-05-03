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

func TestNewFromEnv_Success(t *testing.T) {
	t.Setenv(envProjectID, "proj_env")
	t.Setenv(envAPIKey, "evs_env_secret")

	c, err := NewFromEnv()
	require.NoError(t, err)
	require.Equal(t, "proj_env", c.projectID)
	require.Equal(t, "evs_env_secret", c.apiKey)
}

func TestNewFromEnv_TrimsValues(t *testing.T) {
	t.Setenv(envProjectID, "  proj_env  ")
	t.Setenv(envAPIKey, "\tevs_env_secret\n")

	c, err := NewFromEnv()
	require.NoError(t, err)
	require.Equal(t, "proj_env", c.projectID)
	require.Equal(t, "evs_env_secret", c.apiKey)
}

func TestNewFromEnv_MissingProjectID(t *testing.T) {
	t.Setenv(envProjectID, "")
	t.Setenv(envAPIKey, "evs_env_secret")

	c, err := NewFromEnv()
	require.Error(t, err)
	require.Nil(t, c)
	require.Contains(t, err.Error(), envProjectID)
}

func TestNewFromEnv_WhitespaceProjectID(t *testing.T) {
	t.Setenv(envProjectID, "   ")
	t.Setenv(envAPIKey, "evs_env_secret")

	c, err := NewFromEnv()
	require.Error(t, err)
	require.Nil(t, c)
	require.Contains(t, err.Error(), envProjectID)
}

func TestNewFromEnv_MissingAPIKey(t *testing.T) {
	t.Setenv(envProjectID, "proj_env")
	t.Setenv(envAPIKey, "")

	c, err := NewFromEnv()
	require.Error(t, err)
	require.Nil(t, c)
	require.Contains(t, err.Error(), envAPIKey)
}

func TestNewFromEnv_WhitespaceAPIKey(t *testing.T) {
	t.Setenv(envProjectID, "proj_env")
	t.Setenv(envAPIKey, "  ")

	c, err := NewFromEnv()
	require.Error(t, err)
	require.Nil(t, c)
	require.Contains(t, err.Error(), envAPIKey)
}
