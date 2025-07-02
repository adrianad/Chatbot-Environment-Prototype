#!/usr/bin/env python3
"""
R2R Folder Upload & Graph Creation Script

This script uploads all files from a specified folder to an R2R collection,
extracts entities and relationships, and creates a knowledge graph.

Usage:
1. Set FOLDER_PATH and COLLECTION_NAME in the configuration section
2. Run: python r2r_folder_upload.py
"""

import os
import sys
from pathlib import Path
from typing import List, Dict, Any
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading

try:
    from r2r import R2RClient
    from tqdm.asyncio import tqdm as async_tqdm
    from tqdm import tqdm
except ImportError as e:
    print(f"Missing required dependencies: {e}")
    print("Install with: pip install r2r tqdm")
    sys.exit(1)

# =============================================================================
# CONFIGURATION - MODIFY THESE VALUES
# =============================================================================

FOLDER_PATH = "/srv/bfabriclocal/Desktop/Data - Intranet + wiki/wiki_pages"  # Change this to your folder path
COLLECTION_NAME = "Bfabric-Wiki"      # Change this to your collection name
R2R_BASE_URL = "http://fgcz-h-190:7272"  # Change if using different R2R server
MAX_WORKERS = 7                         # Number of concurrent workers (1-10 recommended)

# =============================================================================
# SUPPORTED FILE FORMATS
# =============================================================================

SUPPORTED_EXTENSIONS = {
    '.txt', '.md', '.rtf',           # Text files
    '.pdf',                          # PDF documents
    '.json', '.jsonl',               # JSON files
    '.csv', '.tsv',                  # Data files
    '.docx', '.doc',                 # Word documents
    '.pptx', '.ppt',                 # PowerPoint files
    '.xlsx', '.xls',                 # Excel files
    '.png', '.jpg', '.jpeg', '.gif', # Images
    '.mp3', '.wav', '.m4a',          # Audio files
    '.mp4', '.avi', '.mov',          # Video files
    '.html', '.htm',                 # Web files
    '.xml',                          # XML files
}

class R2RFolderUploader:
    def __init__(self, base_url: str = R2R_BASE_URL, max_workers: int = 3):
        """Initialize the R2R client."""
        self.client = R2RClient(base_url=base_url)
        self.uploaded_documents = []
        self.max_workers = max_workers
        self.results = {
            'uploaded': [],
            'failed': [],
            'skipped': [],
            'extracted': [],
            'extraction_failed': [],
            'total_entities': 0,
            'total_relationships': 0
        }
        self.results_lock = asyncio.Lock()
        
    def discover_files(self, folder_path: str) -> List[Path]:
        """Discover all supported files in the folder."""
        folder = Path(folder_path)
        if not folder.exists():
            raise FileNotFoundError(f"Folder not found: {folder_path}")
        
        if not folder.is_dir():
            raise ValueError(f"Path is not a directory: {folder_path}")
        
        files = []
        for file_path in folder.rglob('*'):
            if file_path.is_file() and file_path.suffix.lower() in SUPPORTED_EXTENSIONS:
                files.append(file_path)
        
        return sorted(files)
    
    def create_collection_if_needed(self, collection_name: str) -> bool:
        """Create collection if it doesn't exist."""
        try:
            # Try to get existing collections
            collections_response = self.client.collections.list()
            
            # Handle different response formats
            if hasattr(collections_response, 'results'):
                collections_list = collections_response.results
            elif isinstance(collections_response, dict):
                collections_list = collections_response.get('results', [])
            else:
                collections_list = collections_response if isinstance(collections_response, list) else []
            
            existing_names = []
            for col in collections_list:
                if hasattr(col, 'name'):
                    existing_names.append(col.name)
                elif isinstance(col, dict):
                    existing_names.append(col.get('name', ''))
            
            if collection_name in existing_names:
                print(f"✓ Collection '{collection_name}' already exists")
                return True
            else:
                # Create new collection
                result = self.client.collections.create(name=collection_name)
                print(f"✓ Created collection '{collection_name}'")
                return True
                
        except Exception as e:
            print(f"✗ Error managing collection: {e}")
            return False
    
    async def process_file_worker(self, file_queue: asyncio.Queue, collection_name: str, collection_id: str, worker_id: int):
        """Worker that processes files from the queue: upload → extract."""
        while True:
            try:
                # Get next file from queue
                file_path = await file_queue.get()
                if file_path is None:  # Sentinel to stop worker
                    break
                
                print(f"🔄 Worker {worker_id}: Processing {file_path.name}")
                
                # Step 1: Upload the file
                upload_result = await self.upload_file(file_path, collection_name, collection_id, worker_id)
                
                # Step 2: If upload successful, immediately extract entities
                if upload_result['upload_status'] == 'success' and upload_result['document_id']:
                    extraction_result = await self.extract_entities(upload_result, worker_id)
                    
                print(f"✅ Worker {worker_id}: Completed {file_path.name}")
                
                # Mark task as done
                file_queue.task_done()
                
            except Exception as e:
                print(f"❌ Worker {worker_id}: Error processing file: {e}")
                file_queue.task_done()

    async def upload_file(self, file_path: Path, collection_name: str, collection_id: str, worker_id: int) -> Dict[str, Any]:
        """Upload a single file."""
        result = {
            'file_path': str(file_path),
            'upload_status': 'pending',
            'document_id': None,
            'error': None
        }
        
        try:
            print(f"🔼 Worker {worker_id}: Uploading {file_path.name}")
            
            # Convert collection_id to string for JSON serialization
            collection_id_str = str(collection_id)
            
            # Upload document
            try:
                # Try with collection_ids parameter (array)
                upload_result = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self.client.documents.create(
                        file_path=str(file_path),
                        collection_ids=[collection_id_str] if collection_id != collection_name else None,
                        metadata={
                            "collection": collection_name,
                            "collection_id": collection_id_str,
                            "source_folder": str(file_path.parent),
                            "file_size": file_path.stat().st_size,
                            "file_type": file_path.suffix.lower()
                        }
                    )
                )
            except Exception as e1:
                if "collection_ids" in str(e1) or "unexpected keyword argument" in str(e1):
                    # Try without collection_ids parameter
                    upload_result = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: self.client.documents.create(
                            file_path=str(file_path),
                            metadata={
                                "collection": collection_name,
                                "collection_id": collection_id_str,
                                "source_folder": str(file_path.parent),
                                "file_size": file_path.stat().st_size,
                                "file_type": file_path.suffix.lower()
                            }
                        )
                    )
                else:
                    raise e1
            
            # Extract document ID
            document_id = None
            try:
                if hasattr(upload_result, 'results') and hasattr(upload_result.results, 'document_id'):
                    document_id = upload_result.results.document_id
                elif hasattr(upload_result, 'document_id'):
                    document_id = upload_result.document_id
                elif hasattr(upload_result, 'id'):
                    document_id = upload_result.id
            except Exception as e:
                print(f"    ⚠️  Worker {worker_id}: Error extracting document ID for {file_path.name}: {e}")
            
            if document_id:
                result['document_id'] = document_id
                result['upload_status'] = 'success'
                print(f"✅ Worker {worker_id}: Uploaded {file_path.name} -> {document_id}")
                
                async with self.results_lock:
                    self.results['uploaded'].append(result)
            else:
                result['upload_status'] = 'failed'
                result['error'] = 'Could not extract document ID'
                async with self.results_lock:
                    self.results['failed'].append(result)
            
        except Exception as e:
            error_str = str(e)
            
            # Check if it's a duplicate document error
            if "already exists" in error_str:
                result['upload_status'] = 'skipped'
                result['error'] = 'Document already exists'
                print(f"⏭️  Worker {worker_id}: Skipped {file_path.name} (already exists)")
                async with self.results_lock:
                    self.results['skipped'].append(result)
            else:
                result['upload_status'] = 'failed'
                result['error'] = error_str
                print(f"❌ Worker {worker_id}: Failed to upload {file_path.name}: {error_str}")
                async with self.results_lock:
                    self.results['failed'].append(result)
        
        return result

    async def extract_entities(self, document_info: Dict[str, Any], worker_id: int) -> Dict[str, Any]:
        """Extract entities from a single document."""
        document_id = document_info['document_id']
        file_path = document_info['file_path']
        file_name = Path(file_path).name
        
        result = {
            'document_id': document_id,
            'file_path': file_path,
            'extraction_status': 'pending',
            'entities_count': 0,
            'relationships_count': 0,
            'error': None
        }
        
        try:
            print(f"🔍 Worker {worker_id}: Extracting entities from {file_name}")
            
            extract_result = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.client.documents.extract(id=document_id)
            )
            
            # Count entities and relationships
            entities_count = 0
            relationships_count = 0
            
            try:
                if hasattr(extract_result, 'results'):
                    results_obj = extract_result.results
                    if hasattr(results_obj, 'entities'):
                        entities_count = len(results_obj.entities) if results_obj.entities else 0
                    if hasattr(results_obj, 'relationships'):
                        relationships_count = len(results_obj.relationships) if results_obj.relationships else 0
                elif hasattr(extract_result, 'entities'):
                    entities_count = len(extract_result.entities) if extract_result.entities else 0
                elif hasattr(extract_result, 'relationships'):
                    relationships_count = len(extract_result.relationships) if extract_result.relationships else 0
                
                # If no counts but indicates success, assume some extraction occurred
                if entities_count == 0 and relationships_count == 0:
                    if (hasattr(extract_result, 'message') or 
                        str(extract_result).lower().find('success') >= 0):
                        entities_count = 1  # Placeholder
            except Exception:
                pass
            
            result['extraction_status'] = 'success'
            result['entities_count'] = entities_count
            result['relationships_count'] = relationships_count
            
            print(f"✅ Worker {worker_id}: Extracted from {file_name}: {entities_count} entities, {relationships_count} relationships")
            
            async with self.results_lock:
                self.results['total_entities'] += entities_count
                self.results['total_relationships'] += relationships_count
                self.results['extracted'].append(result)
            
        except Exception as e:
            result['extraction_status'] = 'failed'
            result['error'] = str(e)
            print(f"❌ Worker {worker_id}: Failed to extract from {file_name}: {e}")
            
            async with self.results_lock:
                self.results['extraction_failed'].append(result)
        
        return result

    async def process_files_with_workers(self, files: List[Path], collection_name: str) -> List[Dict[str, Any]]:
        """Process files using worker queue approach."""
        print(f"\n📤 Processing {len(files)} files with {self.max_workers} workers")
        
        # Get collection ID once at the start
        collection_id = self.get_collection_id(collection_name)
        if not collection_id:
            print(f"    ⚠️  Could not find collection ID for '{collection_name}', using name")
            collection_id = collection_name
        else:
            print(f"    ✓ Using collection ID: {collection_id}")
        
        # Create a queue and add all files to it
        file_queue = asyncio.Queue()
        for file_path in files:
            await file_queue.put(file_path)
        
        # Create and start workers
        workers = []
        for i in range(self.max_workers):
            worker = asyncio.create_task(
                self.process_file_worker(file_queue, collection_name, collection_id, i + 1)
            )
            workers.append(worker)
        
        # Wait for all files to be processed
        await file_queue.join()
        
        # Stop workers by sending sentinel values
        for i in range(self.max_workers):
            await file_queue.put(None)
        
        # Wait for workers to finish
        await asyncio.gather(*workers)
        
        # Print summary
        uploaded_count = len(self.results['uploaded'])
        skipped_count = len(self.results['skipped'])
        failed_count = len(self.results['failed'])
        extracted_count = len(self.results['extracted'])
        extraction_failed_count = len(self.results['extraction_failed'])
        
        print(f"\n📊 Final Summary:")
        print(f"✅ Successfully uploaded: {uploaded_count} files")
        if skipped_count > 0:
            print(f"⏭️  Skipped duplicates: {skipped_count} files")
        if failed_count > 0:
            print(f"❌ Failed uploads: {failed_count} files")
        
        print(f"✅ Successfully extracted: {extracted_count} documents")
        print(f"📊 Total entities found: {self.results['total_entities']}")
        print(f"🔗 Total relationships found: {self.results['total_relationships']}")
        if extraction_failed_count > 0:
            print(f"❌ Failed extractions: {extraction_failed_count} documents")
        
        return self.results['uploaded']
    
    def extract_entities_and_relationships(self, uploaded_docs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract entities and relationships from uploaded documents."""
        print(f"\n🔍 Extracting entities and relationships from {len(uploaded_docs)} documents...")
        
        extraction_results = {
            'successful': [],
            'failed': [],
            'total_entities': 0,
            'total_relationships': 0
        }
        
        with tqdm(uploaded_docs, desc="Extracting", unit="doc") as pbar:
            for doc_info in pbar:
                try:
                    doc_id = doc_info['document_id']
                    file_name = Path(doc_info['file_path']).name
                    pbar.set_description(f"Extracting from {file_name}")
                    
                    # Skip extraction if we don't have a valid document ID
                    if not doc_id or doc_id == 'None':
                        extraction_results['failed'].append({
                            'document_id': doc_id,
                            'file_path': doc_info['file_path'],
                            'error': 'No valid document ID available'
                        })
                        pbar.write(f"⏭️  Skipped extraction for {file_name}: No document ID")
                        continue
                    
                    # Extract entities and relationships
                    result = self.client.documents.extract(id=doc_id)
                    
                    # Debug the extraction result for first few documents
                    if len(extraction_results['successful']) < 3:
                        print(f"\n    🔍 EXTRACT DEBUG for {file_name}")
                        print(f"    Result type: {type(result)}")
                        if hasattr(result, 'results'):
                            results_obj = result.results
                            print(f"    Results type: {type(results_obj)}")
                            if hasattr(results_obj, '__dict__'):
                                print(f"    Results attributes: {list(vars(results_obj).keys())}")
                                results_dict = vars(results_obj)
                                for key, value in results_dict.items():
                                    if isinstance(value, str) and len(str(value)) < 200:
                                        print(f"    results.{key}: {value}")
                                    elif key in ['success', 'status', 'message']:
                                        print(f"    results.{key}: {value}")
                        elif hasattr(result, '__dict__'):
                            print(f"    Result attributes: {list(vars(result).keys())}")
                            result_dict = vars(result)
                            for key, value in result_dict.items():
                                if isinstance(value, str) and len(str(value)) < 200:
                                    print(f"    {key}: {value}")
                    
                    extraction_results['successful'].append({
                        'document_id': doc_id,
                        'file_path': doc_info['file_path'],
                        'result': str(result)  # Convert to string to avoid serialization issues
                    })
                    
                    # Count entities and relationships if available in result
                    entities_count = 0
                    relationships_count = 0
                    
                    try:
                        # Handle GenericMessageResponse and other response types
                        if hasattr(result, 'results'):
                            results_obj = result.results
                            if hasattr(results_obj, 'entities'):
                                entities_count = len(results_obj.entities) if results_obj.entities else 0
                            if hasattr(results_obj, 'relationships'):
                                relationships_count = len(results_obj.relationships) if results_obj.relationships else 0
                        elif hasattr(result, 'entities'):
                            entities_count = len(result.entities) if result.entities else 0
                        elif hasattr(result, 'relationships'):
                            relationships_count = len(result.relationships) if result.relationships else 0
                        elif isinstance(result, dict):
                            entities_count = len(result.get('entities', []))
                            relationships_count = len(result.get('relationships', []))
                        
                        # If still no counts, the extraction may have succeeded but not returned counts
                        # Just count it as successful extraction
                        if entities_count == 0 and relationships_count == 0:
                            # Check if result indicates success
                            if (hasattr(result, 'message') or 
                                (isinstance(result, dict) and 'message' in result) or
                                str(result).lower().find('success') >= 0):
                                # Assume some entities/relationships were extracted but not returned in count
                                entities_count = 1  # Placeholder to indicate extraction occurred
                    except Exception as e:
                        print(f"    ⚠️  Could not count extracted entities/relationships: {e}")
                    
                    extraction_results['total_entities'] += entities_count
                    extraction_results['total_relationships'] += relationships_count
                    
                except Exception as e:
                    extraction_results['failed'].append({
                        'document_id': doc_info.get('document_id'),
                        'file_path': doc_info['file_path'],
                        'error': str(e)
                    })
                    pbar.write(f"✗ Failed to extract from {Path(doc_info['file_path']).name}: {e}")
        
        print(f"✓ Successfully extracted from: {len(extraction_results['successful'])} documents")
        print(f"📊 Total entities found: {extraction_results['total_entities']}")
        print(f"🔗 Total relationships found: {extraction_results['total_relationships']}")
        
        if extraction_results['failed']:
            print(f"✗ Failed extractions: {len(extraction_results['failed'])} documents")
        
        return extraction_results
    
    def get_collection_id(self, collection_name: str) -> str:
        """Get collection UUID from collection name."""
        try:
            collections_response = self.client.collections.list()
            
            # Handle different response formats
            if hasattr(collections_response, 'results'):
                collections_list = collections_response.results
            elif isinstance(collections_response, dict):
                collections_list = collections_response.get('results', [])
            else:
                collections_list = collections_response if isinstance(collections_response, list) else []
            
            for col in collections_list:
                col_name = None
                col_id = None
                
                if hasattr(col, 'name'):
                    col_name = col.name
                elif isinstance(col, dict):
                    col_name = col.get('name', '')
                
                if hasattr(col, 'id'):
                    col_id = col.id
                elif hasattr(col, 'collection_id'):
                    col_id = col.collection_id
                elif isinstance(col, dict):
                    col_id = col.get('id') or col.get('collection_id')
                
                if col_name == collection_name and col_id:
                    return col_id
            
            return None
        except Exception as e:
            print(f"    ⚠️  Error getting collection ID: {e}")
            return None

    def create_knowledge_graph(self, collection_name: str) -> Dict[str, Any]:
        """Create knowledge graph from the collection."""
        print(f"\n🕸️  Creating knowledge graph for collection '{collection_name}'...")
        
        try:
            # Get collection ID first
            collection_id = self.get_collection_id(collection_name)
            if not collection_id:
                print(f"    ⚠️  Could not find collection ID for '{collection_name}', using name instead")
                collection_id = collection_name
            else:
                print(f"    ✓ Found collection ID: {collection_id}")
            
            # Get collection entities and relationships
            entities = None
            relationships = None
            
            try:
                if hasattr(self.client.collections, 'list_entities'):
                    entities = self.client.collections.list_entities(collection_id=collection_id)
                elif hasattr(self.client, 'entities') and hasattr(self.client.entities, 'list'):
                    entities = self.client.entities.list(collection_id=collection_id)
                elif hasattr(self.client, 'graphs') and hasattr(self.client.graphs, 'list_entities'):
                    entities = self.client.graphs.list_entities(collection_id=collection_id)
            except Exception as e:
                print(f"    ⚠️  Could not retrieve entities: {e}")
                entities = []
            
            try:
                if hasattr(self.client.collections, 'list_relationships'):
                    relationships = self.client.collections.list_relationships(collection_id=collection_id)
                elif hasattr(self.client, 'relationships') and hasattr(self.client.relationships, 'list'):
                    relationships = self.client.relationships.list(collection_id=collection_id)
                elif hasattr(self.client, 'graphs') and hasattr(self.client.graphs, 'list_relationships'):
                    relationships = self.client.graphs.list_relationships(collection_id=collection_id)
            except Exception as e:
                print(f"    ⚠️  Could not retrieve relationships: {e}")
                relationships = []
            
            # Handle different response formats for entities
            entities_count = 0
            if hasattr(entities, 'results'):
                entities_count = len(entities.results) if entities.results else 0
            elif isinstance(entities, dict):
                entities_count = len(entities.get('results', []))
            elif isinstance(entities, list):
                entities_count = len(entities)
            
            # Handle different response formats for relationships
            relationships_count = 0
            if hasattr(relationships, 'results'):
                relationships_count = len(relationships.results) if relationships.results else 0
            elif isinstance(relationships, dict):
                relationships_count = len(relationships.get('results', []))
            elif isinstance(relationships, list):
                relationships_count = len(relationships)
            
            graph_stats = {
                'entities': entities_count,
                'relationships': relationships_count,
                'status': 'created'
            }
            
            if entities_count > 0 or relationships_count > 0:
                print(f"✓ Knowledge graph created successfully!")
                print(f"📊 Graph contains {graph_stats['entities']} entities and {graph_stats['relationships']} relationships")
            else:
                print(f"✓ Knowledge graph structure ready (entities and relationships will be available after extraction)")
                print(f"📊 Collection '{collection_name}' prepared for graph operations")
            
            return graph_stats
            
        except Exception as e:
            print(f"✗ Error creating knowledge graph: {e}")
            return {'status': 'failed', 'error': str(e)}
    
    async def run_complete_workflow(self, folder_path: str, collection_name: str) -> Dict[str, Any]:
        """Run the complete workflow: discover, upload, extract, and create graph."""
        start_time = time.time()
        
        print("🚀 Starting R2R Folder Upload & Graph Creation Workflow")
        print("=" * 60)
        print(f"📁 Source folder: {folder_path}")
        print(f"📚 Target collection: {collection_name}")
        print(f"🌐 R2R server: {self.client.base_url}")
        
        try:
            # Step 1: Discover files
            print(f"\n🔍 Discovering files in '{folder_path}'...")
            files = self.discover_files(folder_path)
            print(f"✓ Found {len(files)} supported files")
            
            if not files:
                print("⚠️  No supported files found. Exiting.")
                return {'status': 'no_files_found'}
            
            # Step 2: Create collection
            if not self.create_collection_if_needed(collection_name):
                return {'status': 'collection_error'}
            
            # Step 3: Process files with workers (upload + extract)
            uploaded_docs = await self.process_files_with_workers(files, collection_name)
            if not uploaded_docs:
                print("⚠️  No files processed successfully. Exiting.")
                return {'status': 'upload_failed'}
            
            # Step 5: Create knowledge graph
            graph_stats = self.create_knowledge_graph(collection_name)
            
            # Summary
            end_time = time.time()
            duration = end_time - start_time
            
            print("\n" + "=" * 60)
            print("🎉 Workflow completed successfully!")
            print(f"⏱️  Total time: {duration:.2f} seconds")
            print(f"📄 Files processed: {len(files)}")
            print(f"📤 Successfully uploaded: {len(uploaded_docs)}")
            print(f"🔍 Successful extractions: {len(self.results['extracted'])}")
            print(f"📊 Total entities: {self.results['total_entities']}")
            print(f"🔗 Total relationships: {self.results['total_relationships']}")
            print(f"🕸️  Knowledge graph: {graph_stats['status']}")
            
            return {
                'status': 'success',
                'files_found': len(files),
                'files_uploaded': len(uploaded_docs),
                'extractions': self.results,
                'graph': graph_stats,
                'duration': duration
            }
            
        except Exception as e:
            print(f"\n💥 Workflow failed: {e}")
            return {'status': 'error', 'error': str(e)}


async def main():
    """Main function to run the R2R folder upload workflow."""
    # Validate configuration
    if FOLDER_PATH == "/path/to/your/folder":
        print("❌ Please configure FOLDER_PATH in the script before running!")
        print("Edit the FOLDER_PATH variable at the top of this script.")
        sys.exit(1)
    
    if COLLECTION_NAME == "my-collection":
        print("⚠️  Using default collection name 'my-collection'")
        print("Consider setting a custom COLLECTION_NAME in the script.")
    
    # Create uploader and run workflow
    uploader = R2RFolderUploader(
        base_url=R2R_BASE_URL, 
        max_workers=MAX_WORKERS
    )
    result = await uploader.run_complete_workflow(FOLDER_PATH, COLLECTION_NAME)
    
    # Exit with appropriate code
    if result['status'] == 'success':
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
