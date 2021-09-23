"""
Submit new data to the exporter 3.0 mock project
"""

import synapseclient
import boto3
import argparse
import pandas

def read_args():
    parser = argparse.ArgumentParser(
        description=("Simulate a new record submission to the "
                     "exporter 3.0 mock project."))
    parser.add_argument(
            "--source-table",
            help="Synapse ID of the exporter 2.0 table where data is being sourced.")
    parser.add_argument(
            "--data-folder",
            help="Synapse ID of the folder where data will be exported to.")
    parser.add_argument(
            "--data-view",
            help="Synapse ID of the entity view which scopes the data folder.")
    parser.add_argument(
            "--limit",
            help="Maximum number of records to export to data folder.")
    parser.add_argument(
            "--query-str",
            help=("An f-string formatted query string to pull filtered data from "
                  "tables in the source project. Use {source_table} "
                  "in FROM clause."))
    parser.add_argument(
            "--file-handle-field",
            help=("The field name in the source tables containing the raw "
                  "data archive."),
            default="rawData")
    args = parser.parse_args()
    return(args)

def anti_join(x, y, on):
    """Return rows in x which are not present in y"""
    ans = pandas.merge(left=x, right=y, how='left', indicator=True, on=on)
    ans = ans.loc[ans._merge == 'left_only', :].drop(columns='_merge')
    return ans

def get_record_diff(syn, source_table, data_view, query_str=None):
    """
    Returns feasible record IDs which exist in source_table but not yet in the
    exporter 3.0 mock project
    """
    if query_str is None:
        query_str = f"SELECT * FROM {source_table}"
    else:
        query_str = query_str.format(source_table = source_table)
    print(query_str)
    source_q = syn.tableQuery(query_str)
    source_df = source_q.asDataFrame()
    target_data = syn.tableQuery(f"SELECT * FROM {data_view}").asDataFrame()
    diff_df = anti_join(source_df, target_data, "recordId")
    return diff_df["recordId"]

def copy_table_data(syn, source_table, data_folder,
        record_ids, limit, file_handle_field):
    record_ids_str = "','".join(record_ids)
    record_ids_str = f"('{record_ids_str}')"
    query_str = (
            f"SELECT * FROM {source_table} where recordId IN {record_ids_str}"
            f"LIMIT {limit}")
    q = syn.tableQuery(query_str)
    file_handles = syn.downloadTableColumns(q, file_handle_field)
    source_df = q.asDataFrame()
    source_df['path'] = source_df[file_handle_field].astype(str).map(file_handles)
    s3_client = boto3.client("s3")
    for _, r in source_df.iterrows():
        annotations = {
                "recordId": str(r["recordId"]),
                "healthCode": str(r["healthCode"]),
                "createdOn": str(r["createdOn"]),
                "taskIdentifier": str(r["metadata.taskIdentifier"]),
                "substudyMemberships": str(r["substudyMemberships"])}
        f = synapseclient.File(
            path = r["path"],
            parent = data_folder,
            annotations = annotations)
        f = syn.store(f)
        # write metadata to S3 object
        copy_source = {
                "Bucket": f["_file_handle"]["bucketName"],
                "Key": f["_file_handle"]["key"]}
        s3_client.copy_object(
                CopySource = copy_source,
                Bucket = copy_source["Bucket"],
                Key = copy_source["Key"],
                Metadata = annotations,
                MetadataDirective="REPLACE")

def main():
    args = read_args()
    syn = synapseclient.login()
    record_diff = get_record_diff(
            syn = syn,
            source_table = args.source_table,
            data_view = args.data_view,
            query_str = args.query_str)
    copy_table_data(
        syn = syn,
        source_table = args.source_table,
        data_folder = args.data_folder,
        record_ids = record_diff,
        limit = args.limit,
        file_handle_field = args.file_handle_field)

if __name__ == "__main__":
    main()
