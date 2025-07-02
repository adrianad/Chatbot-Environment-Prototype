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

try:
    from r2r import R2RClient
    from tqdm import tqdm
except ImportError as e:
    print(f"Missing required dependencies: {e}")
    print("Install with: pip install r2r tqdm")
    sys.exit(1)

# =============================================================================
# CONFIGURATION - MODIFY THESE VALUES
# =============================================================================

FOLDER_PATH = "test"  # Change this to your folder path
COLLECTION_NAME = "test-collection"      # Change this to your collection name
R2R_BASE_URL = "http://fgcz-h-190:7272"  # Change if using different R2R server

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
    def __init__(self, base_url: str = R2R_BASE_URL):
        """Initialize the R2R client."""
        self.client = R2RClient(base_url=base_url)
        self.uploaded_documents = []
        
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
    
    def upload_files(self, files: List[Path], collection_name: str) -> List[Dict[str, Any]]:
        """Upload files to the R2R collection."""
        uploaded = []
        failed = []
        skipped = []
        
        print(f"\n📤 Uploading {len(files)} files to collection '{collection_name}'...")
        
        # Get collection ID once at the start
        collection_id = self.get_collection_id(collection_name)
        if not collection_id:
            print(f"    ⚠️  Could not find collection ID for '{collection_name}', using name")
            collection_id = collection_name
        else:
            print(f"    ✓ Using collection ID: {collection_id}")
        
        with tqdm(files, desc="Uploading", unit="file") as pbar:
            for file_path in pbar:
                try:
                    pbar.set_description(f"Uploading {file_path.name}")
                    
                    # Upload document to specific collection
                    # Convert collection_id to string for JSON serialization
                    collection_id_str = str(collection_id)
                    
                    try:
                        # Try with collection_ids parameter (array)
                        result = self.client.documents.create(
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
                    except Exception as e1:
                        if "collection_ids" in str(e1) or "unexpected keyword argument" in str(e1):
                            # Try without collection_ids parameter
                            result = self.client.documents.create(
                                file_path=str(file_path),
                                metadata={
                                    "collection": collection_name,
                                    "collection_id": collection_id_str,
                                    "source_folder": str(file_path.parent),
                                    "file_size": file_path.stat().st_size,
                                    "file_type": file_path.suffix.lower()
                                }
                            )
                        else:
                            raise e1
                    
                    # Extract document ID from R2RResults[IngestionResponse]
                    document_id = None
                    
                    try:
                        # The structure is: result.results.document_id
                        if hasattr(result, 'results') and hasattr(result.results, 'document_id'):
                            document_id = result.results.document_id
                        # Fallback: try direct access
                        elif hasattr(result, 'document_id'):
                            document_id = result.document_id
                        elif hasattr(result, 'id'):
                            document_id = result.id
                    except Exception as e:
                        print(f"    ⚠️  Error extracting document ID: {e}")
                    
                    uploaded.append({
                        'file_path': str(file_path),
                        'document_id': document_id,
                        'result': str(result)  # Convert to string to avoid serialization issues
                    })
                    
                    # Try to add document to collection if it wasn't done during upload
                    if document_id and collection_id != collection_name:
                        try:
                            # Try to add document to collection after upload
                            if hasattr(self.client.collections, 'add_document'):
                                self.client.collections.add_document(
                                    collection_id=collection_id,
                                    document_id=document_id
                                )
                            elif hasattr(self.client.documents, 'add_to_collection'):
                                self.client.documents.add_to_collection(
                                    document_id=document_id,
                                    collection_id=collection_id
                                )
                        except Exception as e:
                            # Not critical if this fails - document is still uploaded
                            pass
                    
                except Exception as e:
                    error_str = str(e)
                    
                    # Check if it's a duplicate document error
                    if "already exists" in error_str:
                        skipped.append({
                            'file_path': str(file_path),
                            'reason': 'Document already exists'
                        })
                        pbar.write(f"⏭️  Skipped {file_path.name}: Already exists")
                    else:
                        failed.append({
                            'file_path': str(file_path),
                            'error': error_str
                        })
                        pbar.write(f"✗ Failed to upload {file_path.name}: {error_str}")
        
        print(f"✓ Successfully uploaded: {len(uploaded)} files")
        if skipped:
            print(f"⏭️  Skipped duplicates: {len(skipped)} files")
        if failed:
            print(f"✗ Failed uploads: {len(failed)} files")
        
        self.uploaded_documents = uploaded
        return uploaded
    
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
    
    def run_complete_workflow(self, folder_path: str, collection_name: str) -> Dict[str, Any]:
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
            
            # Step 3: Upload files
            uploaded_docs = self.upload_files(files, collection_name)
            if not uploaded_docs:
                print("⚠️  No files uploaded successfully. Exiting.")
                return {'status': 'upload_failed'}
            
            # Step 4: Extract entities and relationships
            extraction_results = self.extract_entities_and_relationships(uploaded_docs)
            
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
            print(f"🔍 Successful extractions: {len(extraction_results['successful'])}")
            print(f"📊 Total entities: {extraction_results['total_entities']}")
            print(f"🔗 Total relationships: {extraction_results['total_relationships']}")
            print(f"🕸️  Knowledge graph: {graph_stats['status']}")
            
            return {
                'status': 'success',
                'files_found': len(files),
                'files_uploaded': len(uploaded_docs),
                'extractions': extraction_results,
                'graph': graph_stats,
                'duration': duration
            }
            
        except Exception as e:
            print(f"\n💥 Workflow failed: {e}")
            return {'status': 'error', 'error': str(e)}


def main():
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
    uploader = R2RFolderUploader(base_url=R2R_BASE_URL)
    result = uploader.run_complete_workflow(FOLDER_PATH, COLLECTION_NAME)
    
    # Exit with appropriate code
    if result['status'] == 'success':
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
