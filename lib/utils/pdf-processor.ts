import fs from 'fs/promises';
import path from 'path';
import pdfParse from 'pdf-parse';
import { db } from '../db';
import { resources } from '../db/schema/resources';
import { generateEmbeddings } from '../ai/embedding';
import { embeddings } from '../db/schema/embeddings';

// Enhanced chunking function for PDFs
const chunkPdfText = (text: string, maxChunkSize: number = 2500): string[] => {
  // Split the text into paragraphs (identified by multiple newlines)
  const paragraphs = text
    .split(/\n{2,}/)
    .map(p => p.replace(/\s+/g, ' ').trim())
    .filter(p => p.length > 0);
  
  const chunks: string[] = [];
  let currentChunk = '';
  
  for (const paragraph of paragraphs) {
    // If adding this paragraph would exceed the max chunk size
    if (currentChunk.length + paragraph.length > maxChunkSize) {
      // If the current chunk is not empty, add it to chunks
      if (currentChunk.length > 0) {
        chunks.push(currentChunk.trim());
        currentChunk = paragraph;
      } 
      // If the paragraph itself is larger than maxChunkSize, we need to split it
      else if (paragraph.length > maxChunkSize) {
        // Split by sentences as a fallback for very large paragraphs
        const sentences = paragraph.split(/\.\s+/).map(s => s.trim() + '.');
        
        for (const sentence of sentences) {
          if (currentChunk.length + sentence.length > maxChunkSize) {
            if (currentChunk.length > 0) {
              chunks.push(currentChunk.trim());
              currentChunk = sentence;
            } else {
              // If a single sentence is too long, we have to split it
              let sentencePart = sentence;
              while (sentencePart.length > maxChunkSize) {
                chunks.push(sentencePart.substring(0, maxChunkSize).trim());
                sentencePart = sentencePart.substring(maxChunkSize);
              }
              currentChunk = sentencePart;
            }
          } else {
            currentChunk += (currentChunk ? ' ' : '') + sentence;
          }
        }
      } else {
        currentChunk = paragraph;
      }
    } else {
      // Add paragraph with a space if current chunk isn't empty
      currentChunk += (currentChunk ? '\n\n' : '') + paragraph;
    }
  }
  
  // Add the last chunk if it's not empty
  if (currentChunk.trim()) {
    chunks.push(currentChunk.trim());
  }
  
  return chunks;
};

export async function processPdf(filePath: string): Promise<string> {
  const dataBuffer = await fs.readFile(filePath);
  const pdfData = await pdfParse(dataBuffer);
  return pdfData.text;
}

export async function processAndStorePdf(filePath: string): Promise<{ resourceId: string, chunks: number }> {
  try {
    // Extract text from PDF
    const pdfText = await processPdf(filePath);
    const fileName = path.basename(filePath);
    
    // Store the full content as a resource
    const [resource] = await db.insert(resources).values({
      content: pdfText,
      source: fileName
    }).returning();
    
    // Generate chunks
    const textChunks = chunkPdfText(pdfText);
    
    // Process each chunk and generate embeddings
    for (const chunk of textChunks) {
      const embeddingResults = await generateEmbeddings(chunk);
      
      // Store each chunk with its embedding
      for (const { content, embedding } of embeddingResults) {
        await db.insert(embeddings).values({
          resourceId: resource.id,
          content,
          embedding
        });
      }
    }
    
    return { resourceId: resource.id, chunks: textChunks.length };
  } catch (error) {
    console.error('Error processing PDF:', error);
    throw error;
  }
}

// Function to process all PDFs in a directory
export async function processAllPdfsInDirectory(directoryPath: string): Promise<Array<{ file: string, result: { resourceId: string, chunks: number } }>> {
  const files = await fs.readdir(directoryPath);
  const pdfFiles = files.filter(file => file.toLowerCase().endsWith('.pdf'));
  
  const results = [];
  
  for (const pdfFile of pdfFiles) {
    const fullPath = path.join(directoryPath, pdfFile);
    const result = await processAndStorePdf(fullPath);
    results.push({ file: pdfFile, result });
  }
  
  return results;
} 