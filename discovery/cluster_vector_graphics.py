#!/usr/bin/env python3

"""
Script to cluster vector graphics elements from a PDF page into coherent groups
that might represent charts, diagrams, or other visual elements.
"""

import argparse
from pathlib import Path
import logging
from typing import List, Dict, Any, Tuple
import json
import numpy as np
from dataclasses import dataclass
from sklearn.cluster import DBSCAN
import pymupdf

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class Rect:
    """Simple rectangle class for handling bounding boxes."""
    x1: float
    y1: float
    x2: float
    y2: float
    
    @property
    def center(self) -> Tuple[float, float]:
        """Get the center point of the rectangle."""
        return ((self.x1 + self.x2) / 2, (self.y1 + self.y2) / 2)
    
    @property
    def width(self) -> float:
        """Get the width of the rectangle."""
        return self.x2 - self.x1
    
    @property
    def height(self) -> float:
        """Get the height of the rectangle."""
        return self.y2 - self.y1
    
    @property
    def area(self) -> float:
        """Get the area of the rectangle."""
        return self.width * self.height
    
    def overlaps(self, other: 'Rect') -> bool:
        """Check if this rectangle overlaps with another."""
        return not (self.x2 < other.x1 or self.x1 > other.x2 or
                   self.y2 < other.y1 or self.y1 > other.y2)
    
    def distance_to(self, other: 'Rect') -> float:
        """Calculate the minimum distance between two rectangles."""
        dx = max(0, min(abs(self.x1 - other.x2), abs(self.x2 - other.x1)))
        dy = max(0, min(abs(self.y1 - other.y2), abs(self.y2 - other.y1)))
        return np.sqrt(dx * dx + dy * dy)

def parse_rect(rect_str: str) -> Rect:
    """Parse a PyMuPDF rect string into our Rect class."""
    # Remove 'Rect(' and ')' and split by comma
    coords = rect_str.replace('Rect(', '').replace(')', '').split(',')
    return Rect(
        float(coords[0]),
        float(coords[1]),
        float(coords[2]),
        float(coords[3])
    )

def cluster_by_proximity(graphics: List[Dict[str, Any]], eps: float = 50.0, min_samples: int = 2) -> Dict[int, List[Dict[str, Any]]]:
    """
    Cluster graphics elements based on spatial proximity using DBSCAN.
    Returns a dictionary mapping cluster IDs to lists of graphics elements.
    """
    # Extract centers of all rectangles for clustering
    centers = []
    valid_graphics = []
    
    for graphic in graphics:
        if graphic.get('rect'):
            rect = parse_rect(str(graphic['rect']))
            centers.append(rect.center)
            valid_graphics.append(graphic)
    
    if not centers:
        return {}
    
    # Convert to numpy array for DBSCAN
    X = np.array(centers)
    
    # Perform clustering
    db = DBSCAN(eps=eps, min_samples=min_samples).fit(X)
    labels = db.labels_
    
    # Group graphics by cluster
    clusters = {}
    for i, label in enumerate(labels):
        if label >= 0:  # Ignore noise points (-1)
            if label not in clusters:
                clusters[label] = []
            clusters[label].append(valid_graphics[i])
    
    return clusters

def analyze_cluster(cluster: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analyze a cluster of graphics elements to determine its characteristics
    and likely type (e.g., chart, diagram, etc.).
    """
    # Count different types of elements
    type_counts = {}
    colors = set()
    stroke_widths = set()
    total_commands = 0
    bboxes = []
    
    for element in cluster:
        # Count element types
        elem_type = element.get('type', 'unknown')
        type_counts[elem_type] = type_counts.get(elem_type, 0) + 1
        
        # Collect unique colors
        if element.get('color'):
            colors.add(str(element['color']))
        
        # Collect unique stroke widths
        if element.get('stroke_width'):
            stroke_widths.add(element['stroke_width'])
        
        # Count total drawing commands
        total_commands += len(element.get('items', []))
        
        # Collect bounding boxes
        if element.get('rect'):
            bboxes.append(parse_rect(str(element['rect'])))
    
    # Calculate overall bounding box
    if bboxes:
        overall_bbox = Rect(
            min(b.x1 for b in bboxes),
            min(b.y1 for b in bboxes),
            max(b.x2 for b in bboxes),
            max(b.y2 for b in bboxes)
        )
    else:
        overall_bbox = None
    
    # Try to determine what this cluster might represent
    likely_type = "unknown"
    if type_counts:
        # Check for chart-like characteristics
        if 'l' in type_counts or 's' in type_counts:  # lines or strokes
            if len(colors) > 1:
                likely_type = "chart"
            elif type_counts.get('l', 0) > 5:  # Many lines
                likely_type = "graph"
        elif 'f' in type_counts and type_counts['f'] > 3:  # filled shapes
            likely_type = "diagram"
    
    return {
        'element_types': type_counts,
        'unique_colors': len(colors),
        'unique_stroke_widths': len(stroke_widths),
        'total_commands': total_commands,
        'bbox': overall_bbox,
        'likely_type': likely_type
    }

def extract_and_cluster_graphics(pdf_path: str, page_num: int) -> Dict[str, Any]:
    """
    Extract vector graphics from a page and cluster them into coherent groups.
    """
    try:
        doc = pymupdf.open(pdf_path)
        if page_num >= len(doc):
            raise ValueError(f"Page number {page_num} out of range. PDF has {len(doc)} pages.")
        
        page = doc[page_num]
        
        # Get all drawing commands
        logger.info("Getting drawing commands...")
        drawings = page.get_drawings()
        logger.info(f"Found {len(drawings)} drawing commands")
        
        # Filter for vector content
        vector_graphics = []
        for idx, drawing in enumerate(drawings):
            draw_type = drawing.get('type', 'unknown')
            if draw_type != 'image':  # We're only interested in vector content
                vector_graphics.append({
                    'index': idx,
                    'type': draw_type,
                    'rect': drawing.get('rect', None),
                    'color': drawing.get('color', None),
                    'stroke_width': drawing.get('width', None),
                    'fill_opacity': drawing.get('fill_opacity', None),
                    'stroke_opacity': drawing.get('stroke_opacity', None),
                    'items': drawing.get('items', [])
                })
        
        # Get text blocks that might be labels
        blocks = page.get_text("dict")["blocks"]
        text_blocks = []
        for block in blocks:
            if block["type"] == 0:  # text block
                text = block.get('lines', [{}])[0].get('spans', [{}])[0].get('text', '').strip()
                try:
                    float(text)
                    is_likely_label = True
                except ValueError:
                    is_likely_label = len(text) < 20
                
                if is_likely_label:
                    text_blocks.append({
                        'type': 'text',
                        'text': text,
                        'rect': Rect(
                            block['bbox'][0],
                            block['bbox'][1],
                            block['bbox'][2],
                            block['bbox'][3]
                        ),
                        'font': block.get('lines', [{}])[0].get('spans', [{}])[0].get('font', 'unknown'),
                        'size': block.get('lines', [{}])[0].get('spans', [{}])[0].get('size', 0)
                    })
        
        # Perform initial clustering of vector graphics
        logger.info("Clustering vector graphics...")
        clusters = cluster_by_proximity(vector_graphics)
        
        # Analyze each cluster
        cluster_analysis = {}
        for cluster_id, elements in clusters.items():
            analysis = analyze_cluster(elements)
            
            # Find associated text blocks
            if analysis['bbox']:
                associated_text = []
                for text in text_blocks:
                    if analysis['bbox'].overlaps(text['rect']):
                        associated_text.append(text)
                analysis['associated_text'] = associated_text
            
            cluster_analysis[cluster_id] = analysis
        
        return {
            'clusters': clusters,
            'analysis': cluster_analysis,
            'text_blocks': text_blocks
        }
        
    except Exception as e:
        logger.error(f"Error processing vector graphics: {e}")
        raise
    finally:
        if 'doc' in locals():
            doc.close()

def main():
    parser = argparse.ArgumentParser(description='Cluster vector graphics from a PDF page')
    parser.add_argument('pdf_path', help='Path to the PDF file')
    parser.add_argument('page_num', type=int, help='Page number to extract (0-based)')
    parser.add_argument('--eps', type=float, default=50.0, help='DBSCAN eps parameter (clustering distance)')
    parser.add_argument('--min-samples', type=int, default=2, help='DBSCAN min_samples parameter')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Create output directory
    artifacts_dir = Path('artifacts')
    artifacts_dir.mkdir(exist_ok=True)
    
    # Create output report file
    pdf_name = Path(args.pdf_path).stem
    report_path = artifacts_dir / f"{pdf_name}_page{args.page_num}_clustered_graphics_report.txt"
    
    try:
        # Extract and cluster graphics
        results = extract_and_cluster_graphics(args.pdf_path, args.page_num)
        
        # Generate report
        with open(report_path, 'w') as f:
            f.write(f"Clustered Graphics Report for {pdf_name}, page {args.page_num}\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"Found {len(results['clusters'])} potential graphic groups\n\n")
            
            for cluster_id, elements in results['clusters'].items():
                f.write(f"Cluster {cluster_id}:\n")
                f.write("-" * 40 + "\n")
                
                analysis = results['analysis'][cluster_id]
                f.write(f"Likely type: {analysis['likely_type']}\n")
                f.write(f"Element types: {analysis['element_types']}\n")
                f.write(f"Unique colors: {analysis['unique_colors']}\n")
                f.write(f"Unique stroke widths: {analysis['unique_stroke_widths']}\n")
                f.write(f"Total drawing commands: {analysis['total_commands']}\n")
                
                if analysis.get('associated_text'):
                    f.write("\nAssociated text elements:\n")
                    for text in analysis['associated_text']:
                        f.write(f"  - {text['text']} (font: {text['font']}, size: {text['size']})\n")
                
                f.write("\nElements:\n")
                for elem in elements:
                    f.write(f"  - Type: {elem['type']}, ")
                    if elem.get('color'):
                        f.write(f"Color: {elem['color']}, ")
                    if elem.get('stroke_width'):
                        f.write(f"Stroke: {elem['stroke_width']}, ")
                    if elem.get('rect'):
                        f.write(f"Rect: {elem['rect']}")
                    f.write("\n")
                
                f.write("\n")
        
        logger.info(f"Cluster analysis report saved to: {report_path}")
        
    except Exception as e:
        logger.error(f"Failed to analyze vector graphics: {e}")
        raise

if __name__ == "__main__":
    main() 