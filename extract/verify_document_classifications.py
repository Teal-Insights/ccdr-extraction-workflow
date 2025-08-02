#!/usr/bin/env python3
"""
Verification script to check document classifications.

This script will:
1. Show all publications with their documents and types
2. Verify exactly one MAIN document per publication
3. Flag any potential classification issues
4. Generate a summary report

Run with: uv run verify_document_classifications.py
"""

from typing import List, Tuple
from sqlmodel import Session, select
from sqlalchemy import text

from load.db import engine
from load.schema import Publication, Document, DocumentType


def get_all_publications_with_documents() -> List[Tuple[Publication, List[Document]]]:
    """Get all publications with their associated documents."""
    with Session(engine) as session:
        # Get all publications
        publications = session.exec(select(Publication)).all()
        
        result = []
        for pub in publications:
            # Get documents for this publication
            docs = session.exec(
                select(Document)
                .where(Document.publication_id == pub.id)
                .order_by(Document.id)
            ).all()
            result.append((pub, docs))
        
        return result


def print_detailed_report():
    """Print a detailed report of all publications and their documents."""
    print("📚 DETAILED PUBLICATION & DOCUMENT REPORT")
    print("=" * 80)
    
    pub_docs = get_all_publications_with_documents()
    
    for pub, docs in pub_docs:
        print(f"\n📖 {pub.title}")
        print(f"   ID: {pub.id} | Year: {pub.publication_date} | Documents: {len(docs)}")
        print("   " + "-" * 70)
        
        if not docs:
            print("   ⚠️  NO DOCUMENTS FOUND")
            continue
            
        for i, doc in enumerate(docs):
            status_icon = "🎯" if doc.type == DocumentType.MAIN else "📄" if doc.type == DocumentType.SUPPLEMENTAL else "❓"
            print(f"   {status_icon} [{doc.type.value:12}] {doc.description}")
            if i == 0 and doc.type != DocumentType.MAIN:
                print("      ⚠️  First document is not MAIN")


def check_main_document_counts():
    """Check that each publication has exactly one MAIN document."""
    print("\n🔍 MAIN DOCUMENT COUNT VERIFICATION")
    print("=" * 50)
    
    with Session(engine) as session:
        # Query to count MAIN documents per publication
        result = session.exec(text("""
            SELECT 
                p.id,
                p.title,
                COUNT(CASE WHEN d.type = 'MAIN' THEN 1 END) as main_count,
                COUNT(d.id) as total_docs
            FROM publication p
            LEFT JOIN document d ON p.id = d.publication_id
            GROUP BY p.id, p.title
            ORDER BY main_count, p.title;
        """))
        
        rows = result.fetchall()
        
        perfect_count = 0
        issues = []
        
        for row in rows:
            pub_id, title, main_count, total_docs = row
            
            if main_count == 1:
                perfect_count += 1
                print(f"✅ {title[:60]}... (1 MAIN, {total_docs} total)")
            elif main_count == 0:
                issues.append(f"❌ NO MAIN: {title}")
                print(f"❌ {title[:60]}... (0 MAIN, {total_docs} total)")
            else:
                issues.append(f"❌ MULTIPLE MAIN: {title} ({main_count} MAIN docs)")
                print(f"❌ {title[:60]}... ({main_count} MAIN, {total_docs} total)")
        
        print("\n📊 SUMMARY:")
        print(f"   ✅ Perfect publications: {perfect_count}")
        print(f"   ❌ Publications with issues: {len(issues)}")
        
        if issues:
            print("\n🚨 ISSUES FOUND:")
            for issue in issues:
                print(f"   {issue}")
            
            return False
        else:
            print("\n🎉 ALL PUBLICATIONS HAVE EXACTLY ONE MAIN DOCUMENT!")
            return True


def analyze_classification_patterns():
    """Analyze classification patterns to spot potential issues."""
    print("\n📊 CLASSIFICATION PATTERN ANALYSIS")
    print("=" * 50)
    
    with Session(engine) as session:
        # Count by type
        result = session.exec(text("""
            SELECT type, COUNT(*) as count
            FROM document
            GROUP BY type
            ORDER BY count DESC;
        """))
        
        type_counts = result.fetchall()
        total = sum(row.count for row in type_counts)
        
        print(f"📈 Document Type Distribution ({total} total):")
        for row in type_counts:
            percentage = (row.count / total) * 100
            print(f"   {row.type:12}: {row.count:4} ({percentage:5.1f}%)")
        
        # Check for suspicious patterns
        print("\n🔍 Pattern Analysis:")
        
        # Check for publications with all SUPPLEMENTAL (no MAIN)
        result = session.exec(text("""
            SELECT p.title, COUNT(d.id) as doc_count
            FROM publication p
            JOIN document d ON p.id = d.publication_id
            WHERE p.id NOT IN (
                SELECT DISTINCT publication_id 
                FROM document 
                WHERE type = 'MAIN'
            )
            GROUP BY p.id, p.title;
        """))
        
        no_main_pubs = result.fetchall()
        if no_main_pubs:
            print(f"   ⚠️  Publications with no MAIN documents: {len(no_main_pubs)}")
            for pub in no_main_pubs[:5]:  # Show first 5
                print(f"      • {pub.title}")
        else:
            print("   ✅ All publications have at least one MAIN document")


def main():
    """Main verification function."""
    print("🔍 Document Classification Verification Tool")
    print("=" * 80)
    
    try:
        # Detailed report
        print_detailed_report()
        
        # Main document count verification
        all_good = check_main_document_counts()
        
        # Pattern analysis
        analyze_classification_patterns()
        
        if all_good:
            print("\n🎉 VERIFICATION PASSED!")
            print("All publications have exactly one MAIN document.")
        else:
            print("\n⚠️  VERIFICATION FAILED!")
            print("Some publications don't have exactly one MAIN document.")
            print("You may need to re-run the recovery script or manually fix classifications.")
            
    except Exception as e:
        print(f"\n❌ Verification failed: {e}")


if __name__ == "__main__":
    main()