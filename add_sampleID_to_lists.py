import os
import pandas as pd
import synapseclient

SAMPLE_FILES_DIR = "/home/cmolitor/bsmn_reprocessing/tjb_raw_sample_lists"
OUTPUT_FILE = "/home/cmolitor/bsmn_reprocessing/manifest_files/reprocessed_sampleID.csv"
REPROCESSED_GRANT_FILE = "/home/cmolitor/bsmn_reprocessing/reprocessed_grant_data.csv"

FILE_SAMPLE_COLUMNS = ["file_sample_id", "data_file", "data_file_path"]
DATAFILE_COLUMNS = ["data_file1", "data_file2", "data_file3", "data_file4"]
FILE_EXTENSIONS = [".cram", ".cram.crai", ".flagstat.txt", ".ploidy_2.vcf.gz",
                   ".ploidy_2.vcf.gz.tbi", ".ploidy_12.vcf.gz", ".ploidy_12.vcf.gz.tbi",
                   ".ploidy_50.vcf.gz", ".ploidy_50.vcf.gz.tbi",".unmapped.bam"]

SYN_GUID_SAMPLES = "syn20822548"

datafile_df = pd.DataFrame()
expanded_sample_df = pd.DataFrame()
file_list_df = pd.DataFrame()
fileext_df = pd.DataFrame()
no_samples_df = pd.DataFrame()

syn = synapseclient.Synapse()
syn.login(silent=True)

# Get the LIVE sample file from the GUID API and split it into separate lines
# per file to make it easier to compare to Taejeong's sample files.
guid_sample_file = open(syn.get(SYN_GUID_SAMPLES).path)
guid_sample_df = pd.read_csv(guid_sample_file)
guid_sample_df.columns = guid_sample_df.columns.str.lower()

for col_var in DATAFILE_COLUMNS:
    datafile_df = datafile_df.append(guid_sample_df, ignore_index=True)
    datafile_df["orig_file"] = datafile_df[col_var]
    datafile_df.dropna(subset=["orig_file"], how="all", inplace=True)
    expanded_sample_df = expanded_sample_df.append(datafile_df, ignore_index=True, sort=False)
    datafile_df = pd.DataFrame()

expanded_sample_df["orig_file"] = expanded_sample_df["orig_file"].str.strip("]]>")
expanded_sample_df["data_file"] = expanded_sample_df["orig_file"].apply(os.path.basename)
expanded_sample_df = expanded_sample_df[["sample_id_original", "data_file"]]

# Get a list of the sample list files from Taejeong
sample_lists = []
tjb_filenames = os.listdir(SAMPLE_FILES_DIR)
for filename in tjb_filenames:
    sample_lists.append(os.path.join(SAMPLE_FILES_DIR, filename))

for list_file in sample_lists:
    tjb_sample_file = open(list_file)
    tjb_sample_df = pd.read_csv(tjb_sample_file, sep="\t", names=FILE_SAMPLE_COLUMNS)
    tjb_sample_df = tjb_sample_df[["file_sample_id", "data_file"]]

    # Create the data file list including the correct sample ID.
    combined_sample_df = pd.merge(expanded_sample_df, tjb_sample_df, how="outer",
                                  on="data_file", indicator=True)
    file_list_df = file_list_df.append(combined_sample_df.loc[combined_sample_df["_merge"] == "both"],
                                       ignore_index=True)

file_list_df = file_list_df[["file_sample_id", "sample_id_original"]]

# Create files with the necessary extensions to match the resubmitted files.
for file_ext in FILE_EXTENSIONS:
    ext_df = pd.DataFrame()
    ext_df = file_list_df.copy()
    ext_df["name"] = ext_df["file_sample_id"] + file_ext
    ext_df = ext_df[["sample_id_original", "name"]]

    fileext_df = fileext_df.append(ext_df, ignore_index=True)

fileext_df = fileext_df.drop_duplicates(subset=["name"])
fileext_df = fileext_df.rename(columns={"sample_id_original": "sample_id_used"})
fileext_df = fileext_df[["sample_id_used", "name"]]

# Read in the data downloaded from Kenny's reprocessed grant data table.
reprocessed_df = pd.read_csv(open(REPROCESSED_GRANT_FILE, "r"))
reprocessed_df.drop("sample_id_used", axis=1, inplace=True)
new_reprocessed_df = pd.merge(reprocessed_df, fileext_df, how="left", on="name")

output_file = open(OUTPUT_FILE, "w")
new_reprocessed_df.to_csv(output_file, index=False)
output_file.close()
