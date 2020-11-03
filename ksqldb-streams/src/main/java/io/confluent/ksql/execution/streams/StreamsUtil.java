/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.execution.context.QueryContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StreamsUtil {

  private static final Logger LOG = LoggerFactory.getLogger(StreamsUtil.class);

  private static int uniqueId = 0;

  private StreamsUtil() {
  }

  public static String buildOpName(final QueryContext opContext) {
    LOG.info("SOPHIE: at unique id {}", uniqueId);
    ++uniqueId;
    return String.join("-", opContext.getContext()) + "-" + uniqueId;
  }
}
