/**
 * @brief 
 * @author huangpeng
 * @version 
 * @date 2015-10-27
 */
package com.sample.bulk;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author hadoop
 *
 */

public class WholeFilelnputFormat extends InputFormat{

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public RecordReader createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
	 */
	@Override
	public List getSplits(JobContext arg0) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

}
