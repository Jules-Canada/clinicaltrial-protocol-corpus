#Create a training dataset of publicly available clinical trial protocols
Begin with https://clinicaltrials.gov/
Create a list of all trial NCTIDs
For each NCTID, identify all study documents. For an example of an NCTID with a protocol document, see https://cdn.clinicaltrials.gov/large-docs/27/NCT01295827/Prot_SAP_000.pdf
Start with a sample of 50 trial NCTIDs and protocols
Consider how we can store the full dataset

##Public
Project intended to share via public github
Scrape https://clinicaltrials.gov/ info in a responsible manner:
- API calls (clinicaltrials.gov/api/v2): 1.0s delay — matches robots.txt Crawl-delay
- PDF downloads (cdn.clinicaltrials.gov): 0.2s delay — Google Cloud Storage CDN, no stated limit
Output Format
Primary working format: JSONL (one record per protocol)
Final release format: Parquet (for Hugging Face dataset publishing)
Raw PDFs are processed in memory and discarded — not saved to disk at scale.
Use --save-pdfs flag during local dev/testing to cache PDFs under pdfs/{NCT_ID}/

###JSONL Schema (in order)
nct_id — e.g. NCT01295827
title — official study title
phase — e.g. PHASE3
conditions — list of conditions/diseases
sponsor — lead sponsor name
source_url — PDF URL the text was extracted from
retrieved_date — ISO date of download
page_count — number of pages in source PDF
sections — dict of detected protocol sections and their text
full_text — complete extracted text