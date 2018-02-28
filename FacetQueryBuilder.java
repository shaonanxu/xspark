package cn.net.ycloud.ydb.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.StringHelper;


public class FacetQueryBuilder {
	
	public static final Logger LOG = Logger.getLogger(FacetQueryBuilder.class);
	
	public static final int LIMIT = 10240;
	
	public static enum SortType{
		DESC (DescPriorityQueue.class) {
			@Override
			public boolean isInQueue(Tuple top, Tuple n) {
				return top.count < n.count;
			}
		}, 
		ASC (AscPriorityQueue.class) {
			@Override
			public boolean isInQueue(Tuple top, Tuple n) {
				return top.count > n.count;
			}
		}, 
		LEX (LexPriorityQueue.class) {
			@Override
			public boolean isInQueue(Tuple top, Tuple n) {
				return top.a.compareTo(n.a) < 0 ? false : true;
			}
		};
		
		private Class<? extends PriorityQueue<Tuple>> queueClazz;
		private SortType(Class<? extends PriorityQueue<Tuple>> queueClazz){
			this.queueClazz = queueClazz;
		}
		PriorityQueue<Tuple> newQueue(int capacity) {
			try {
				return queueClazz.getConstructor(Integer.class).newInstance(capacity);
			} catch (Exception e) {
			}
			return null;
		}
		
		public abstract boolean isInQueue(Tuple top, Tuple n);
	}
	
	private class DescPriorityQueue extends PriorityQueue<Tuple>{
		public DescPriorityQueue(int maxSize) {
			super(maxSize);
		}
		@Override
		protected boolean lessThan(Tuple a, Tuple b) {
			return a.count > b.count;
		}
	}
	
	private class AscPriorityQueue extends PriorityQueue<Tuple>{
		public AscPriorityQueue(int maxSize) {
			super(maxSize);
		}
		@Override
		protected boolean lessThan(Tuple a, Tuple b) {
			return a.count < b.count;
		}
	}
	
	/** lex 字典序排序 */
	private class LexPriorityQueue extends PriorityQueue<Tuple>{
		public LexPriorityQueue(int maxSize) {
			super(maxSize);
		}
		@Override
		protected boolean lessThan(Tuple a, Tuple b) {
			return a.a.compareTo(b.a) > 0 ? false : true;
		}
	}
	
	private LeafReader reader;
	private String[] fields;
	private FieldInfo[] fieldInfos;
	private BitSet baseDocs;
	private String prefix;
	private int minCount;
	private SortType sortType;
	private String[] facetQueries;
	private int limit;
	
	public FacetQueryBuilder(LeafReader reader, BitSet baseDocs){
		this.reader = reader;
		this.baseDocs = baseDocs;
	}
	
	/**
	 * 支持聚合返回值前缀匹配
	 * eg:
	 * 	 abc(10), ac(10), bc(10)
	 *   prefix=a
	 *   只返回 abc(10), ac(10)
	 */
	public void setPrefix(String prefix){
		this.prefix = prefix;
	}
	
	/**
	 * 返回聚合后最小值限制
	 *  eg:
	 * 	 abc(10), ac(10), bc(6)
	 *   min=10
	 *   只返回 abc(10), ac(10)
	 */
	public void setMinCount(int min){
		this.minCount = min;
	}
	
	public void setLimit(int limit){
		this.limit = limit;
	}
	
	public void setFacetQueries(String[] facetQueries){
		this.facetQueries = facetQueries;
	}
	
	public void setSort(SortType sortType){
		this.sortType = sortType;
	}
	
	public void setFacetFields(String[] fields, FieldInfo[] fieldInfos){
		this.fields = fields;
		this.fieldInfos = fieldInfos;
	}
	
	public FacetSearcher<?> build(){
		if(this.facetQueries != null && this.facetQueries.length > 0){
			return new QueryFacetSearcher();
		}
		return new DefaultFacetSearcher();
	}

	public static interface FacetSearcher<V> {
		public V search();
	}
	
	private class QueryFacetSearcher implements FacetSearcher<List<Tuple>> {
		
		private static final String KEY = "!{key";
		private static final int LIMIT = 5;
		
		@Override
		public List<Tuple> search() {
			final BitSet baseDocs = FacetQueryBuilder.this.baseDocs;
			final int minCount = FacetQueryBuilder.this.minCount;
			final SortType sortType = FacetQueryBuilder.this.sortType;
			final String[] facetQueries = FacetQueryBuilder.this.facetQueries;
			final LeafReader leafReader = FacetQueryBuilder.this.reader;
			try {
				List<Tuple> ret = new ArrayList<>();
				Query luceneQuery = null;
				for(int i=0;i<LIMIT;i++){
					String facetQuery = facetQueries[i];
					String key = null;
					int p = 0;
					if(facetQuery.startsWith(KEY)){
						p = facetQuery.indexOf("}", 6);
						key = facetQuery.substring(6, p);
						luceneQuery = this.parseLuceneQuery(facetQuery.substring(p+1));
					} else {
						key = facetQuery.substring(p+1);
						luceneQuery = this.parseLuceneQuery(key);
					}
					IndexSearcher searcher = new IndexSearcher(leafReader);
					FacetCollector collector = new FacetCollector(baseDocs, leafReader.maxDoc());
					searcher.search(luceneQuery, collector);
					int count = collector.count();
					if(count> minCount) {
						ret.add(new Tuple(key, count));
					}
				}
				if(sortType != null){
					Collections.sort(ret, new Comparator<Tuple>() {
						@Override
						public int compare(Tuple o1, Tuple o2) {
							return sortType.isInQueue(o1, o2) ? 1 : -1;
						}
					});
				}
				return ret;
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
		
		private Query parseLuceneQuery(String query) throws IOException{
			Query q = null;
			return q;
		}
		
		class FacetCollector implements Collector{
			BitSet bitSet;
			FacetCollector(BitSet bitSet, int maxDoc){
			}
			@Override
			public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
				return new LeafCollector(){
					@Override
					public void setScorer(Scorer scorer) throws IOException {
					}
					@Override
					public void collect(int doc) throws IOException {
					}
				};
			}

			@Override
			public boolean needsScores() {
				return false;
			}
			int count(){
				return this.bitSet.length();
			}
		}
	}
	
	private class DefaultFacetSearcher implements FacetSearcher<Map<String, List<Tuple>>> {
		@Override
		public Map<String, List<Tuple>> search() {
			final BitSet baseDocs = FacetQueryBuilder.this.baseDocs;
			final int minCount = FacetQueryBuilder.this.minCount;
			final SortType sortType = FacetQueryBuilder.this.sortType;
			final int limit = FacetQueryBuilder.this.limit == 0 ? LIMIT : FacetQueryBuilder.this.limit;
			Map<String, List<Tuple>> ret = new HashMap<>();
			int len = Math.min(FacetQueryBuilder.this.fields.length, FacetQueryBuilder.this.fieldInfos.length);
			for(int i=0;i<len;i++){
				String field = FacetQueryBuilder.this.fields[i];
				List<Tuple> list = new ArrayList<>();
				FacetIterator fit = 
						FacetQueryBuilder.this.newFacetIterator(FacetQueryBuilder.this.reader, baseDocs, field, FacetQueryBuilder.this.fieldInfos[i], FacetQueryBuilder.this.prefix);
				if(fit == null){
					LOG.warn(" *** Error Facet Field *** ");
					continue;
				}
				if(sortType == null) {
					for(;fit.hasNext()&&list.size()<limit;){
						if(fit.count() > minCount){
							list.add(new Tuple(fit.next(), fit.count()));
						}
					}
				} else {
					PriorityQueue<Tuple> queue = sortType.newQueue(limit);
					int in = 0;
					Tuple top = null;
					for(;fit.hasNext();){
						if(fit.count() > minCount){
							Tuple a = new Tuple(fit.next(), fit.count());
							if(in == limit){
								if(sortType.isInQueue(top, a)){
									top.a = a.a;
									top.count = a.count;
									top = queue.updateTop();
								}
							} else {
								top = queue.add(a);
								in ++;
							}
						}
					}
					Tuple t = queue.pop();
					while(t != null){
						list.add(t);
						t = queue.pop();
					}
				}
				ret.put(field, list);
			}
			return ret;
		}
	}
	
	private FacetIterator newFacetIterator(LeafReader reader, BitSet liveDocs, String field, FieldInfo fieldInfo, String prefix){
		IndexOptions indexOptions = fieldInfo.getIndexOptions();
		if(indexOptions == IndexOptions.DOCS) {
			TermsEnum te;
			try {
				te = reader.terms(field).iterator();
				if(prefix == null)
					return new TermsEnumFacetIterator(te, liveDocs);
				else 
					return new PrefixTermsEnumFacetIterator(te, liveDocs, prefix);
			} catch (IOException e) {
				LOG.error(" *** LeafReader("+reader.getContext().toString()+") read TermsEnum error *** ", e);
			}
		} else {
			DocValuesType dvt = fieldInfo.getDocValuesType();
			if(dvt == DocValuesType.BINARY){
				
			} else if(dvt == DocValuesType.NUMERIC){
				
			} else if(dvt == DocValuesType.SORTED){
				try {
					SortedDocValues sdv = reader.getSortedDocValues(field);
				} catch (IOException e) {
					LOG.error(" *** LeafReader("+reader.getContext().toString()+") read DocValues error *** ", e);
				}
			}
		}
		return null;
	}
	
	/**
	 * 倒排表生成facet结果
	 */
	private class TermsEnumFacetIterator implements FacetIterator {
		protected TermsEnum te;
		protected BytesRef term;
		private BitSet liveDocs;
		
		TermsEnumFacetIterator(TermsEnum te, BitSet liveDocs){
			this.te = te;
			this.liveDocs = liveDocs;
		}
		
		@Override
		public int count() {
			try {
				if(liveDocs == null){
					return te.docFreq();
				} else {
					BitSetIterator it = new BitSetIterator(this.liveDocs, 0);
					PostingsEnum pe = te.postings(null);
					int doc = it.nextDoc();
					int count = 0;
					while(doc != BitSetIterator.NO_MORE_DOCS){
						int _doc = pe.advance(doc);
						if(_doc == doc){
							count ++;
						}
					}
					return count;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return 0;
		}
		@Override
		public boolean hasNext() {
			try {
				this.term = te.next();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return term != null;
		}
		@Override
		public String next() {
			return this.term.utf8ToString();
		}
	}
	
	private class PrefixTermsEnumFacetIterator extends TermsEnumFacetIterator{
		private BytesRef prefix;
		private boolean valid;
		PrefixTermsEnumFacetIterator(TermsEnum te, BitSet liveDocs, String prefix) {
			super(te, liveDocs);
			this.prefix = new BytesRef(prefix);
			try {
				this.valid = te.seekCeil(this.prefix) != SeekStatus.END;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		@Override
		public boolean hasNext() {
			if(!valid) return false;
			try {
				this.term = te.next();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return term != null && StringHelper.startsWith(term, prefix);
		}
	}
	
	private class DocValuesFacetIterator implements FacetIterator {
		
		DocValuesFacetIterator(){
			
		}

		@Override
		public int count() {
			return 0;
		}

		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public String next() {
			return null;
		}
		
	}
	
	private interface FacetIterator {
		public int count();
		public boolean hasNext();
		public String next();
	}
	
	public class Tuple {
		public String a;
		public int count;
		public Tuple(String a, int count){
			this.a = a;
			this.count = count;
		}
		@Override
		public String toString() {
			return this.a + "(" + this.count + ")";
		}
	}
}
