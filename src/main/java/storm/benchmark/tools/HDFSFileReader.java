/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package storm.benchmark.tools;

import java.io.IOException;
import storm.benchmark.util.FileUtils;

import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import static storm.benchmark.lib.spout.FileReadSpout.conf;

public class HDFSFileReader implements Serializable {

  private static final long serialVersionUID = -7012334600647556269L;

  public final String file;
  private List<String> contents;
  private int index = 0;
	private int limit = 0;

  public HDFSFileReader(String file) {
    this.file = file;
    if (this.file != null) {
          try {
              this.contents = FileUtils.readLines(FileSystem.get(URI.create(this.file), conf)
                      .open(new Path(this.file)).getWrappedStream());
              this.limit = contents.size();
          } catch (IOException ex) {
              Logger.getLogger(FileReader.class.getName()).log(Level.SEVERE, null, ex);
          }
    } else {
      throw new IllegalArgumentException("file name cannot be null");
    }
  }

  public String nextLine() {
    if (index >= limit) {
	    index = 0;
	  }
    String line = contents.get(index);
    index++;
    return line;
  }


}

