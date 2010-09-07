package org.apache.hadoop.mapreduce.lib.input;

import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * This class has been created to allow {@link MSCRMapper} to access a FileSplit when it is wrapped by a 
 * TaggedInputSplit. Because the latter is a private class, the only way to access it is by implementing this
 * business logic in the org.apache.hadoop.mapreduce.lib.input class space.
 */
public class FileInputSplitWrapper {

  public static FileSplit getFileInputSplit(Context context) {
    // This class is private!
    TaggedInputSplit t = (TaggedInputSplit)context.getInputSplit();
    FileSplit fS = (FileSplit)t.getInputSplit();
    return fS;
  }
}
