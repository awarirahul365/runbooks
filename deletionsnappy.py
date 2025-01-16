#!/usr/bin/env python3
import sys
import traceback
from datetime import datetime
from uuid import uuid4

import automationassets
from afs_snappy import (
    AFSManager,
    Alerting,
    AlertType,
    CustomLogger,
    IdentityManager,
    Snapshot,
    StorageManager,
    TICHttpBasedAlerting,
    Watcher,
    get_cid,
    get_current_datetime,
)

# Constants
RUNBOOK_TYPE = "afs-snapshots-deletion"
VERSION = "1.0.0"

# automation variables
subscription_id = automationassets.get_automation_variable("SUBSCRIPTION_ID")
resource_group = automationassets.get_automation_variable("RESOURCE_GROUP")
object_storage = automationassets.get_automation_variable("OBJECT_STORAGE")
exclude_afs = automationassets.get_automation_variable("EXCLUDE_AFS")

logger = CustomLogger(__name__)

deleted_snapshots = 0


def __has_snapshot_expired(
    current_date_time: datetime, snapshot: Snapshot
) -> bool:  # noqa
    """__has_snapshot_expired
    Checks if snapshot is expired

    :rtype: bool
    """

    time_difference = current_date_time - snapshot.created_at
    created_since = int(time_difference.days)

    return created_since > snapshot.retention_days


def __delete_snapshot(
    snapshot: Snapshot,
    afs_manager: AFSManager,
    watcher: Watcher,
    correlation_id: str,
    logger: CustomLogger,
    alerting: Alerting,
) -> bool:
    """__delete_snapshot

    delete snapshot and skip if the snapshot
    wasn't deleted for some reason

    :rtype: None
    """

    global deleted_snapshots

    current_date_time = get_current_datetime()

    try:
        with watcher.watch(f"Delete snapshot: {snapshot.name}", correlation_id):  # noqa
            afs_manager.delete_snapshot(snapshot)

            deleted_snapshots = deleted_snapshots + 1

            return True
    except Exception as ex:
        error_message = f"Skipping: {snapshot.name}, Unable to delete the snapshot with exception: {ex} - correlation_id={correlation_id}"  # noqa

        # Sending TIC alert for failure in snapshot deletion
        alerting.send(
            type=AlertType.FAIL,
            start_time=current_date_time,
            message=error_message,  # noqa
        )  # noqa

        logger.warning(error_message)

        return False


def __delete_snapshots_in_afs(
    afs_manager: AFSManager,
    correlation_id: str,
    current_date_time: datetime,
    watcher: Watcher,
    alerting: Alerting,
):
    try:
        # Fetch snapshots
        logger.info("Fetching snapshots list")
        snapshot_list = afs_manager.get_snapshots()

        # Make sure if there are snapshots to delete
        if snapshot_list.total < 1:
            logger.info(
                f"There are no snapshots in AFS '{afs_manager.afs_name}' - correlation_id={correlation_id}"  # noqa
            )  # noqa
            return

        logger.info(
            f"{snapshot_list.total} Snapshots found; Iterating over snapshots list to check for expired snapshots"  # noqa
        )

        deleted_snapshots_in_afs = 0

        for snapshot in snapshot_list.snapshots:
            if snapshot.retention_days and snapshot.retention_days >= 0:
                is_expired = __has_snapshot_expired(current_date_time, snapshot)  # noqa

                if is_expired:
                    logger.info(f"Deleting snapshot {snapshot.name}")
                    snapshot_deleted = __delete_snapshot(
                        snapshot,
                        afs_manager,
                        watcher,
                        correlation_id,
                        logger,
                        alerting=alerting,
                    )

                    if snapshot_deleted:
                        deleted_snapshots_in_afs = deleted_snapshots_in_afs + 1

        logger.info(
            f"'{deleted_snapshots_in_afs}' snapshot(s) deleted in AFS ‘{afs_manager.afs_name}‘ - correlation_id={correlation_id}"  # noqa
        )
    except Exception as ex:
        error_message = f"Skipping: '{afs_manager.afs_name}' due to exception: {ex} - correlation_id={correlation_id}"  # noqa

        # Sending TIC alert for failure in snapshot deletion
        alerting.send(
            type=AlertType.FAIL,
            start_time=current_date_time,
            message=error_message,  # noqa
        )

        logger.warning(error_message)


# Execution starts from here
def main():
    """main
    Execution starts from here

    :rtype: None
    """

    # Until job id is not there, we will use our own uuid for correlation
    correlation_id = str(uuid4())

    current_date_time = get_current_datetime()

    # Watcher object to log time it took to complete the operation(s)
    watcher = Watcher(logger)

    logger.info(
        f"Starting snapshot listing and deletion Job - correlation_id={correlation_id}"  # noqa
    )  # noqa

    logger.info(f"Type: {RUNBOOK_TYPE}")
    logger.info(f"Script version: {VERSION}")

    cid = get_cid(resource_group)
    logger.info(f"Customer ID: {cid}")

    # Initializing Alerting
    alerting: Alerting = TICHttpBasedAlerting(
        logger=logger,
        account_id=subscription_id,
        cid=cid,
        hostname=object_storage,
        script_version=VERSION,
        correlation_id=correlation_id,
        sid = "SID",
        object_storage=object_storage,
    )

    try:
        logger.info("Fetching managed identity token")
        identity_token = IdentityManager.get_managed_identity_token(
            correlation_id
        )  # noqa

        storage_manager = StorageManager(
            subscription_id=subscription_id,
            resource_group=resource_group,
            object_storage=object_storage,
            identity_token=identity_token,
            correlation_id=correlation_id,
        )

        # Validates storage for:
        # - validates subscription id
        # - validates object storage
        logger.info(
            f"Validating storage dependencies: Subscription ID, Object Storage: '{object_storage}'"  # noqa
        )
        storage_manager.validate_storage()

        # Fetching AFS list available in storage account
        logger.info(f"Fetching AFS volumes in storage: '{object_storage}'")
        afs_list_in_storage = storage_manager.get_afs_list(exclude_afs)

        if afs_list_in_storage.total < 1:
            logger.info(
                f"No AFS volumes found in AFS storage '{object_storage}'. Exiting"  # noqa
            )
            sys.exit(0)

        logger.info(
            f"Total AFS found in the storage '{object_storage}' are: '{afs_list_in_storage.total}'"  # noqa
        )

        # Iterating the list of AFS volumes available in Storage account # noqa
        logger.info("Iterating the list of AFS volumes found...\n")

        for afs_name in afs_list_in_storage.afs_list:
            afs_manager = AFSManager(
                identity_token=identity_token,
                storage_manager=storage_manager,
                afs_name=afs_name,
                correlation_id=correlation_id,
            )

            logger.add_seperator()

            # Validates if afs exists
            logger.info("Validating AFS exists")
            afs_manager.validate_afs_exists()

            # Validate AFS and delete snapshots in afs
            __delete_snapshots_in_afs(
                afs_manager,
                correlation_id,
                current_date_time,
                watcher,
                alerting,  # noqa
            )

            # Sending TIC Alert after the successful operations of deleting snapshots # noqa
            alerting.send(type=AlertType.SUCCESS, start_time=current_date_time, db_name=afs_name)  # noqa

        logger.add_seperator()

        logger.info(
            f"Snapshot listing and deletion job completed with total {deleted_snapshots} snapshot(s) deleted - correlation_id={correlation_id}"  # noqa
        )

    except Exception as ex:
        traceback.print_exc()

        error_message = f"An error occurred while executing AFS listing and deletion runbook: {ex} - correlation_id={correlation_id}"  # noqa

        # Sending TIC alert for the failed message
        alerting.send(
            type=AlertType.FAIL,
            start_time=current_date_time,
            message=error_message,  # noqa
        )

        logger.error(error_message)
        sys.exit(1)


if __name__ == "__main__":
    main()
