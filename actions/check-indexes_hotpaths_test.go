package actions

import (
	"path/filepath"
	"testing"
)

func TestDeviceLegacyArmIndexFileDeclaresOwnershipBounds(t *testing.T) {
	path := filepath.Join("..", "indexes", "migration-hub-device-legacy-arm-05-10-2026.txt")
	canonical, err := loadCanonicalIndexSpecsFromFile(path)
	if err != nil {
		t.Fatalf("loadCanonicalIndexSpecsFromFile: %v", err)
	}

	specs := canonical["devices"]
	if len(canonical) != 1 || len(specs) != 1 ||
		normalizeKey(specs[0].Key) != "user_id:1.organisationId:1.projectId:1.key:1" ||
		specs[0].Name != "user_id_1_organisationId_1_projectId_1_key_1" ||
		specs[0].Unique {
		t.Fatalf("device legacy arm index specs = %#v", canonical)
	}
}

func TestMarkerOptionValuesInRangeIndexFileDeclaresEndFirstContract(t *testing.T) {
	path := filepath.Join("..", "indexes", "migration-hub-marker-option-values-in-range-05-10-2026.txt")
	canonical, err := loadCanonicalIndexSpecsFromFile(path)
	if err != nil {
		t.Fatalf("loadCanonicalIndexSpecsFromFile: %v", err)
	}
	if len(canonical) != 3 {
		t.Fatalf("collections = %#v", canonical)
	}
	for _, collection := range []string{"marker_option_ranges", "marker_tag_option_ranges", "marker_event_option_ranges"} {
		specs := canonical[collection]
		if len(specs) != 1 ||
			normalizeKey(specs[0].Key) != "organisationId:1.projectId:1.end:1.start:1.value:1" ||
			specs[0].Name != "organisationId_1_projectId_1_end_1_start_1_value_1" ||
			specs[0].Unique {
			t.Fatalf("%s specs = %#v", collection, specs)
		}
	}
}

func TestNotificationStatisticsIndexFileDeclaresCoveredCountContract(t *testing.T) {
	path := filepath.Join("..", "indexes", "migration-hub-notification-statistics-06-10-2026.txt")
	canonical, err := loadCanonicalIndexSpecsFromFile(path)
	if err != nil {
		t.Fatalf("loadCanonicalIndexSpecsFromFile: %v", err)
	}

	specs := canonical["notifications"]
	if len(canonical) != 1 || len(specs) != 1 ||
		normalizeKey(specs[0].Key) != "organisationId:1.alert_master_user:1.userid:1.projectId:1.read:1.device_id:1" ||
		specs[0].Name != "organisationId_1_alert_master_user_1_userid_1_projectId_1_read_1_device_id_1" ||
		specs[0].Unique {
		t.Fatalf("notification statistics index specs = %#v", canonical)
	}
}
