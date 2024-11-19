/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.service.auth;

import org.apache.hadoop.hive.thrift.TFilterTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.security.auth.Subject;
import java.util.concurrent.CompletionException;

/**
 *
 * This is used on the client side, where the API explicitly opens a transport to
 * the server using the Subject.doAs()
 */
public class TSubjectAssumingTransport extends TFilterTransport {

    public TSubjectAssumingTransport(TTransport wrapped) {
        super(wrapped);
    }

    @Override
    public void open() throws TTransportException {
        try {
            Subject subject = Subject.current();
            Subject.callAs(subject, () -> {
                wrapped.open();
                return null;
            });
        }
        catch (CompletionException rte) {
            if (rte.getCause() instanceof TTransportException) {
                throw (TTransportException)rte.getCause();
            } else {
                throw rte;
            }
        }
    }
}
