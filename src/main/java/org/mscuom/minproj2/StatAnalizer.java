package org.mscuom.minproj2;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StatAnalizer  implements Serializable{
	static Log LOG = LogFactory.getLog(StatAnalizer.class);
    /**
	 * 
	 */
	private static final long serialVersionUID = 3133837446705915833L;
	private  String exception;
    private   String msg;
    private  int occurence ;
    
    public StatAnalizer(String exception, String msg) {
      this.exception = exception;
      this.msg = msg;
    }
    private StatAnalizer(String exception,String msg,int count) {
	      this.exception = exception;
	      this.msg = msg;
	    }
    public StatAnalizer merge(StatAnalizer other) {
    	LOG.info("------  merging "+this.toString() +" with -->"+other.toString());
    	if(other.exception!=null){
    	if(this.exception.equalsIgnoreCase(other.exception) && this.msg.equalsIgnoreCase(other.msg)) {
    		LOG.info("Same occurence found ");
    		++this.occurence;
    		return new StatAnalizer(other.exception,other.msg,this.occurence);
    	} 
    	}
      return other;
    }

    public String toString() {
      return "exception:"+exception+", Message : "+msg + " number of occurence : "+occurence;
    		  
    }
  

}
