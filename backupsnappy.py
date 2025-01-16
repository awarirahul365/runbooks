#!/usr/bin/env python3

import sys
import traceback
from datetime import datetime
from typing import Callable
from uuid import uuid4

import automationassets
from afs_snappy import (
    AFSManager,
    Alerting,
    AlertType,
    CustomLogger,
    IdentityManager,
    StorageManager,
    TICHttpBasedAlerting,
    Watcher,
    get_cid,
    get_current_datetime,
    validate_is_numeric,
)

# Constants
RUNBOOK_TYPE = "afs-backup"
VERSION = "1.0.0"

AFS_SOFTDELETE_RETENTION_DAYS = 7
AFS_TOTAL_SNAPSHOTS_LIMIT = 200


# automation variables
subscription_id = automationassets.get_automation_variable("SUBSCRIPTION_ID")
resource_group = automationassets.get_automation_variable("RESOURCE_GROUP")
object_storage = automationassets.get_automation_variable("OBJECT_STORAGE")
exclude_afs = automationassets.get_automation_variable("EXCLUDE_AFS")
retention_days = automationassets.get_automation_variable("RetentionDays")


correlation_id = ""
triggered_from_vm = ""
sid = "SID"

allowed_adhoc_backup_retention_days_list = [
    3,
    7,
    15,
    30,
    45,
    60,
    67,
    90,
    180,
    365,
    730,
    1095,
    1460,
    1825,
]

is_adhoc_backup = False

# Adhoc backup retention days passed
if len(sys.argv) > 1:
    retention_days = str(sys.argv[1])

    is_adhoc_backup = True

# correlation-id is passed
if len(sys.argv) > 2:
    correlation_id = str(sys.argv[2])

# vm information is passed
if len(sys.argv) > 3:
    triggered_from_vm = str(sys.argv[3])

# sid is passed
if len(sys.argv) > 4:
    sid = str(sys.argv[4])

logger = CustomLogger(__name__)

snapshots_created = 0


def __validate_allowed_adhoc_backup_retention_days(rdays: str):
    """__validate_allowed_adhoc_backup_retention_days
    Validates if the retention days is allowed by
    comparing with the list of adhoc backup retention
    days

    Raise exception if retenion days is not in the allowed list

    :rtype: None
    """

    retenion_days_int = int(rdays)

    if retenion_days_int not in allowed_adhoc_backup_retention_days_list:
        raise Exception(
            f"The retention days for adhoc backup must be from: [{', '.join(str(backup_retention_day) for backup_retention_day in allowed_adhoc_backup_retention_days_list)}]"  # noqa
        )

def __enable_softdelete_in_file_share(
    storage_manager: StorageManager,
    correlation_id: str,
    current_date_time: datetime,
    watcher: Watcher,
    alerting: Alerting,
):
    """__enable_softdelete_in_file_share
    Validate if softdelete is enabled.
    Enable softdelete option in file share if it is not enabled

    :rtype: None
    """

    try:
        logger.info(
            f"Checking if soft delete is enabled for file share service in '{storage_manager.object_storage}' storage account..."  # noqa
        )

        if storage_manager.is_softdelete_enabled():
            logger.info(
                f"Soft delete is already enabled for file share service in '{storage_manager.object_storage}' storage account. Skipping"  # noqa
            )
            return

        # Enable afs softdelete option in Azure file share
        logger.info(
            f"Enabling soft delete for file share service in '{storage_manager.object_storage}' with Retention days: {AFS_SOFTDELETE_RETENTION_DAYS}"  # noqa
        )

        with watcher.watch(
            f"Enable soft delete for file share service in '{storage_manager.object_storage}'",  # noqa
            correlation_id,
        ):
            storage_manager.enable_file_share_softdelete(
                AFS_SOFTDELETE_RETENTION_DAYS
            )  # noqa
    except Exception as ex:
        error_message = f"Skipping softdelete enablement for file share service in '{storage_manager.object_storage}' due to exception: {ex} - correlation_id={correlation_id}"  # noqa

        # Sending TIC alert for failure in snapshot deletion
        alerting.send(
            type=AlertType.FAIL,
            start_time=current_date_time,
            message=error_message,  # noqa
        )

        logger.warning(error_message)


def __create_afs_snapshot(
    afs_manager: AFSManager,
    correlation_id: str,
    current_date_time: datetime,
    watcher: Watcher,
    alerting: Alerting,
    is_adhoc_bkp: bool,
):  # noqa
    """__create_snapshot
    Validate afs snapshot storage limit and create snapshoz

    :rtype: None
    """

    global snapshots_created

    try:
        # Validates if there is storage for more snapshot backups
        logger.info(
            f"Validating AFS snapshot storage limit not exceed {AFS_TOTAL_SNAPSHOTS_LIMIT} in '{afs_manager.afs_name}'"  # noqa
        )
        afs_manager.validate_afs_snapshots_storage_limit(
            AFS_TOTAL_SNAPSHOTS_LIMIT
        )  # noqa

        # Create snapshot backup
        logger.info(
            f"Creating {'Adhoc' if is_adhoc_bkp else 'Automated'} snapshot backup of AFS '{afs_manager.afs_name}' with Retention days: {retention_days}"  # noqa
        )  # noqa

        with watcher.watch("Create snapshot", correlation_id):
            afs_manager.create_snapshot(retention_days, is_adhoc_bkp)

            snapshots_created = snapshots_created + 1
    except Exception as ex:
        error_message = f"Skipping create snapshot in: '{afs_manager.afs_name}' due to exception: {ex} - correlation_id={correlation_id}"  # noqa

        # Sending TIC alert for failure in snapshot deletion
        alerting.send(
            type=AlertType.FAIL,
            start_time=current_date_time,
            message=error_message,  # noqa
        )  # noqa

        logger.warning(error_message)


# Execution starts from here
def main():
    """main
    Execution starts from here

    :rtype: None
    """

    # Until job id is not there, we will use our own uuid for correlation
    global correlation_id
    if not correlation_id:
        correlation_id = str(uuid4())

    current_date_time = get_current_datetime()

    # Watcher object to log time it took to complete the operation(s)
    watcher = Watcher(logger)

    logger.info(
        f"Starting {'Adhoc' if is_adhoc_backup else 'Automated'} snapshot creation job - correlation_id={correlation_id}"  # noqa
    )

    logger.info(f"Type: {RUNBOOK_TYPE}")
    logger.info(f"Script version: {VERSION}")

    cid = get_cid(resource_group)
    logger.info(f"Customer ID: {cid}")

    if triggered_from_vm:
        hostname = triggered_from_vm
    else:
        hostname = object_storage

    # Initializing Alerting
    alerting: Alerting = TICHttpBasedAlerting(
        logger=logger,
        account_id=subscription_id,
        cid=cid,
        hostname=hostname,
        script_version=VERSION,
        correlation_id=correlation_id,
        sid=sid,
        object_storage=object_storage,
    )

    try:
        # Validating the retention days
        validate_is_numeric(retention_days, "RetentionDays")

        if is_adhoc_backup:
            __validate_allowed_adhoc_backup_retention_days(retention_days)

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
        )  # noqa
        storage_manager.validate_storage()

        # Enable softdelete in file share service if it is not enabled
        __enable_softdelete_in_file_share(
            storage_manager,
            correlation_id,
            current_date_time,
            watcher,
            alerting,  # noqa
        )

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

        # Iterating over the list of AFS volumes available in Storage account # noqa
        logger.info("Iterating over the list of AFS volumes found...\n")
        for afs_name in afs_list_in_storage.afs_list:
            afs_manager = AFSManager(
                identity_token=identity_token,
                storage_manager=storage_manager,
                afs_name=afs_name,
                correlation_id=correlation_id,
            )

            logger.add_seperator()

            # Validates if afs exists
            logger.info(f"Validating AFS '{afs_manager.afs_name}'")
            afs_manager.validate_afs_exists()

            if is_adhoc_backup:
                logger.info(
                    f"Skipping the check if backup is already created today for AFS '{afs_manager.afs_name}' as the job is adhoc backup"  # noqa
                )
            else:
                if afs_manager.is_snapshot_already_created_today():
                    logger.info(
                        f"Automated Snapshot for AFS '{afs_manager.afs_name}' is already created for today. Skipping..."  # noqa
                    )
                    continue

            # Validate storage exists for snapshots and create snapshot
            __create_afs_snapshot(
                afs_manager,
                correlation_id,
                current_date_time,
                watcher,
                alerting,
                is_adhoc_backup,
            )

            # Sending the TIC Alert after the successful creation of the snapshot
            alerting.send(
                type=AlertType.SUCCESS, 
                start_time=current_date_time,
                db_name=afs_name
            )

        logger.add_seperator()

        logger.info(
            f"Snapshot(s) creation job completed with total '{snapshots_created}' snapshots created - correlation_id={correlation_id}"  # noqa
        )
    except Exception as ex:
        traceback.print_exc()

        error_message = f"An error occurred while executing AFS backup runbook: {ex} - correlation_id={correlation_id}"  # noqa

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
