using Carubbi.Extensions;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.BR;
using Lucene.Net.Documents;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Lucene.Net.Store;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Version = Lucene.Net.Util.Version;

namespace Carubbi.ElasticSearch
{
    public class ElasticSearcher
    {

        private readonly IndexSearcher _searcher;

        public ElasticSearcher(string path)
        {
            Directory directory = FSDirectory.Open(path);
            _searcher = new IndexSearcher(directory);
        }

        public T GetById<T>(Guid id)
        {
            var parser = new QueryParser(Version.LUCENE_30, "#id", CreateAnalyzer());
            var qry = parser.Parse(id.ToString());

            var collector = TopScoreDocCollector.Create(1, true);
            _searcher.Search(qry, collector);
            var hits = collector.TopDocs().ScoreDocs;
            var docId = hits[0].Doc;
            var doc = _searcher.Doc(docId);

            var item = Activator.CreateInstance<T>();
            foreach (var field in doc.GetFields())
            {
                item.SetProperty(field.Name, field.StringValue);
            }

            return item;
        }

        public IEnumerable<T> Search<T>(string terms, params string[] fields)
        {
            //QueryParser parser = new MultiFieldQueryParser(Lucene.Net.Util.Version.LUCENE_30, fields, CreateAnalyzer());
            //Query query = parser.Parse(terms);
            //var query = parser.Parse(string.Format("\"{0}\"", terms));

            var query = MultiFieldQueryParser.Parse(Version.LUCENE_30, new[] {terms},
                fields, CreateAnalyzer());
            var results = _searcher.Search(query, int.MaxValue);

            foreach (var result in results.ScoreDocs.OrderByDescending(d => d.Score))
            {
                yield return CastDocument<T>(_searcher.Doc(result.Doc));
            }
        }

        private static Analyzer CreateAnalyzer()
        {
            return new BrazilianAnalyzer(Version.LUCENE_30);
        }

        private static T CastDocument<T>(Document doc)
        {
            var item = Activator.CreateInstance<T>();
            foreach (var field in doc.GetFields())
            {
                if (field.Name == "#id")
                    continue;

                var converter = TypeDescriptor.GetConverter(item.GetType().GetProperty(field.Name)?.PropertyType ?? throw new InvalidOperationException());
                item.SetProperty(field.Name, converter.ConvertFromString(field.StringValue));
            }
            return item;
        }
    }
}
