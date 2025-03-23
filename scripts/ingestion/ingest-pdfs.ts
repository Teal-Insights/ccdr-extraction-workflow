/**
 * PDF Ingestion Script
 * 
 * This script provides utilities to process PDF files and store them for retrieval.
 * You can process either a single PDF file or all PDFs in a directory.
 * 
 * Usage:
 * 1. To process a single file: 
 *    `tsx scripts/ingestion/ingest-pdfs.ts --file=./data/example.pdf`
 * 
 * 2. To process all PDFs in a directory:
 *    `tsx scripts/ingestion/ingest-pdfs.ts --dir=./data`
 */

import { processAndStorePdf, processAllPdfsInDirectory } from '@/lib/utils/pdf-processor';
import path from 'path';

async function main() {
  const args = process.argv.slice(2);
  const fileArg = args.find(arg => arg.startsWith('--file='));
  const dirArg = args.find(arg => arg.startsWith('--dir='));

  if (fileArg) {
    const filePath = fileArg.split('=')[1];
    console.log(`Processing file: ${filePath}`);
    const result = await processAndStorePdf(filePath);
    console.log(`Processed resource ${result.resourceId} with ${result.chunks} chunks`);
  } else if (dirArg) {
    const dirPath = dirArg.split('=')[1];
    console.log(`Processing all PDFs in directory: ${dirPath}`);
    const results = await processAllPdfsInDirectory(dirPath);
    console.log(`Processed ${results.length} PDF files`);
    
    // Print detailed results
    results.forEach(item => {
      console.log(`- ${item.file}: ${item.result.chunks} chunks (resourceId: ${item.result.resourceId})`);
    });
  } else {
    console.log('Usage:');
    console.log('  Process a single file: tsx scripts/ingestion/ingest-pdfs.ts --file=./data/example.pdf');
    console.log('  Process all files in a directory: tsx scripts/ingestion/ingest-pdfs.ts --dir=./data');
  }
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error('Error during PDF ingestion:', error);
    process.exit(1);
  }); 