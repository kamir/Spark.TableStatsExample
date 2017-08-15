package com.cloudera.sa.examples.tablestats.model

import scala.collection.mutable

import org.apache.solr.client.solrj.impl.CloudSolrClient

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

/**
 * Created by ted.malaska on 6/29/15.
 */
class FirstPassStatsModel extends Serializable {
  var columnStatsMap = new mutable.HashMap[Integer, ColumnStats]

  def +=(colIndex: Int, colValue: Any, colCount: Long): Unit = {
    columnStatsMap.getOrElseUpdate(colIndex, new ColumnStats) += (colValue, colCount)
  }

  def +=(firstPassStatsModel: FirstPassStatsModel): Unit = {
    firstPassStatsModel.columnStatsMap.foreach{ e =>
      val columnStats = columnStatsMap.getOrElse(e._1, null)
      if (columnStats != null) {
        columnStats += (e._2)
      } else {
        columnStatsMap += ((e._1, e._2))
      }
    }
  }

  override def toString = s"FirstPassStatsModel(columnStatsMap=$columnStatsMap)"

  def storeInCollection( colName : String ) {
	val zkHostString = "cc-poc-mk-3.gce.cloudera.com:2181,cc-poc-mk-2.gce.cloudera.com:2181,cc-poc-mk-1.gce.cloudera.com:2181/solr";
        val solrClient = new CloudSolrClient.Builder().withZkHost(zkHostString).build();
        
        
        columnStatsMap.foreach( x => {
        
           println(s"key: ${x._1}, value: ${x._2}")
           
           val doc = new SolrInputDocument();
        
        
        
           solrClient.add(doc);
        
        } )

        solrClient.commit(); 

  }


}
