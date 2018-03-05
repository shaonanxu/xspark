package test.com.x.xpark_hadoop.lucene.facet;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.junit.BeforeClass;
import org.junit.Test;

import com.x.xpark_hadoop.lucene.facet.FacetQueryBuilder;
import com.x.xpark_hadoop.lucene.facet.FacetQueryBuilder.Tuple;

public class TestFacetQueryBuilder {
	
	public static FieldType fieldType(IndexOptions indexOption, DocValuesType docValuesType) {
		FieldType ft = new FieldType();
		ft.setDocValuesType(docValuesType);
		ft.setIndexOptions(indexOption);
		return  ft;
	}
	
	public static FieldType fieldType(IndexOptions indexOption, DocValuesType docValuesType, NumericType ntype) {
		FieldType ft = fieldType(indexOption, docValuesType);
		ft.setNumericType(ntype);
		return  ft;
	}
	
	RAMDirectory d;
	LeafReader leafReader;
	
	final FieldType A = fieldType(IndexOptions.DOCS, DocValuesType.NONE);
	final FieldType B = fieldType(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, DocValuesType.NONE);
	final FieldType C = fieldType(IndexOptions.NONE, DocValuesType.NONE, NumericType.INT);
	final FieldType D = fieldType(IndexOptions.NONE, DocValuesType.SORTED);
	
	public void init() throws IOException{
		d = new RAMDirectory();
		IndexWriterConfig conf = new IndexWriterConfig(new StandardAnalyzer());
		conf.setCommitOnClose(true);
		IndexWriter iw = new IndexWriter(d, conf);
		iw.addDocument(doc("a1", "b1", 1));
		iw.addDocument(doc("b1", "b2", 2));
		iw.addDocument(doc("a1", "b3", 3));
		iw.addDocument(doc("c1", "b3", 4));
		iw.addDocument(doc("c1", "b2", 5));
		iw.addDocument(doc("a1", "b5", 6));
		iw.forceMerge(1);
		iw.close();
		IndexSearcher dr = new IndexSearcher(DirectoryReader.open(d));
		IndexReader ir = dr.getIndexReader();
		leafReader = ir.leaves().get(0).reader();
	}
	
	private Document doc(String a, String b, int c){
		Document r = new Document();
		r.add(new StringField("a", a, Store.YES));
		r.add(new TextField("b", b, Store.YES));
		r.add(new IntField("c", c, Store.YES));
		r.add(new SortedDocValuesField("d", new BytesRef(a)));
		return r;
	}
	
	public void testNumericDefaultFacet() throws IOException {
		FacetQueryBuilder builder = new FacetQueryBuilder(leafReader, null);
		builder.setFacetFields(new String[]{"c"}, new FieldType[]{C});
		outputFacetResult("testNumericDefaultFacet", (Map<String, List<Tuple>>) builder.build().search());
	}
	
	public void testDefaultFacet() throws IOException {
		FacetQueryBuilder builder = new FacetQueryBuilder(leafReader, null);
		builder.setFacetFields(new String[]{"a"}, new FieldType[]{A});
		outputFacetResult("testDefaultFacet", (Map<String, List<Tuple>>) builder.build().search());
	}
	
	public void testSearchDefaultFacet() throws IOException{
		FacetCollector collector = new FacetCollector(leafReader.maxDoc());
		IndexSearcher searcher = new IndexSearcher(leafReader);
		searcher.search(new TermQuery(new Term("b", "b2")), collector);
		FacetQueryBuilder builder = new FacetQueryBuilder(leafReader, collector.bitSet);
		builder.setFacetFields(new String[]{"a"}, new FieldType[]{A});
		outputFacetResult("testSearchDefaultFacet", (Map<String, List<Tuple>>) builder.build().search());
	}
	
	public void testPrefixFacet() throws IOException{
		FacetQueryBuilder builder = new FacetQueryBuilder(leafReader, null);
		builder.setFacetFields(new String[]{"a"}, new FieldType[]{A});
		builder.setPrefix("a");
		outputFacetResult("testPrefixFacet", (Map<String, List<Tuple>>) builder.build().search());
	}
	
	public void testSearchPrefixFacet() throws IOException{
		FacetCollector collector = new FacetCollector(leafReader.maxDoc());
		IndexSearcher searcher = new IndexSearcher(leafReader);
		searcher.search(new TermQuery(new Term("b", "b1")), collector);
		FacetQueryBuilder builder = new FacetQueryBuilder(leafReader, collector.bitSet);
		builder.setFacetFields(new String[]{"d"},new FieldType[]{D});
		outputFacetResult("testSearchPrefixFacet", (Map<String, List<Tuple>>) builder.build().search());
	}
	
	public void testSortedDocValuesFacet() throws IOException{
		FacetQueryBuilder builder = new FacetQueryBuilder(leafReader, null);
		builder.setFacetFields(new String[]{"d"}, new FieldType[]{D});
		outputFacetResult("testSortedDocValuesFacet", (Map<String, List<Tuple>>) builder.build().search());
	}
	
	public void testSearchSortedDocValuesFacet() throws IOException{
		FacetCollector collector = new FacetCollector(leafReader.maxDoc());
		IndexSearcher searcher = new IndexSearcher(leafReader);
		searcher.search(new TermQuery(new Term("b", "b3")), collector);
		FacetQueryBuilder builder = new FacetQueryBuilder(leafReader, collector.bitSet);
		builder.setFacetFields(new String[]{"d"}, new FieldType[]{D});
		outputFacetResult("testSearchSortedDocValuesFacet", (Map<String, List<Tuple>>) builder.build().search());
	}
	
	public void testQuerySearchFacet(){
		FacetQueryBuilder builder = new FacetQueryBuilder(leafReader);
		builder.setFacetQueries(new String[]{"a","b"}, 
				new Query[]{new TermQuery(new Term("a", "a1")), new TermQuery(new Term("b", "b2"))});
		outputFacetResult("testQuerySearchFacet", (List<Tuple>) builder.build().search());
	}
	
	public void testSearchQuerySearchFacet() throws IOException{
		FacetCollector collector = new FacetCollector(leafReader.maxDoc());
		IndexSearcher searcher = new IndexSearcher(leafReader);
		searcher.search(new TermQuery(new Term("b", "b1")), collector);
		FacetQueryBuilder builder = new FacetQueryBuilder(leafReader, collector.bitSet);
		builder.setFacetQueries(new String[]{"query:a","query:b"}, 
				new Query[]{new TermQuery(new Term("a", "a1")), new TermQuery(new Term("b", "b2"))});
		outputFacetResult("testSearchQuerySearchFacet", (List<Tuple>) builder.build().search());
	}
	
	class FacetCollector implements Collector {
		public BitSet bitSet;
		public FacetCollector(int maxDoc){
			this.bitSet = new FixedBitSet(maxDoc);
		}
		@Override
		public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
			return new LeafCollector(){
				@Override
				public void setScorer(Scorer scorer) throws IOException {
				}
				@Override
				public void collect(int doc) throws IOException {
					bitSet.set(doc);
				}
			};
		}
		@Override
		public boolean needsScores() {
			return false;
		}
	}
	
	public static void outputFacetResult(String key, Map<String, List<Tuple>> ret){
		System.out.println("======" + key + "======");
		if(ret == null || ret.size() == 0){
			System.out.println(" Result is Empty");
		} else {
			for(Map.Entry<String, List<Tuple>> entry : ret.entrySet()){
				System.out.println("key:"+entry.getKey());
				for(Tuple t : entry.getValue()){
					System.out.println(t.toString());
				}
			}
		}
	}
	
	public static void outputFacetResult(String key, List<Tuple> ret){
		System.out.println("======" + key + "======");
		if(ret == null || ret.size() == 0){
			System.out.println(" Result is Empty");
		} else {
			for(Tuple t : ret){
				System.out.println(t.toString());		
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		TestFacetQueryBuilder builder = new TestFacetQueryBuilder();
		builder.init();
//		builder.testNumericDefaultFacet();
		builder.testDefaultFacet();
		builder.testSearchDefaultFacet();
		builder.testPrefixFacet();
		builder.testSearchPrefixFacet();
		builder.testSortedDocValuesFacet();
		builder.testSearchSortedDocValuesFacet();
		builder.testQuerySearchFacet();
		builder.testSearchQuerySearchFacet();
	}

}
