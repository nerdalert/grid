package main

import "testing"

func TestEnumConstSuffixSupportsSlashSeparatedVersionedValue(t *testing.T) {
	if got, want := enumConstSuffix("dynamo/frontend-v1"), "DynamoFrontendV1"; got != want {
		t.Fatalf("enumConstSuffix() = %q, want %q", got, want)
	}
}

func TestWellKnownScoringStrategyNameSurvivesAddedEnumValue(t *testing.T) {
	schema := schemaNode{
		Type: "string",
		Enum: []interface{}{"noMetrics", "queueDepth", "kvCachePressure", "dynamoRequestPressure"},
	}
	if got, want := wellKnownEnumName(schema, "GridNetworkScoringPolicyStrategy"), "ScoringStrategy"; got != want {
		t.Fatalf("wellKnownEnumName() = %q, want %q", got, want)
	}
}
