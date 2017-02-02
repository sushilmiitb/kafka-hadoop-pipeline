package com.chymeravr.analytics.eventjoin;

import com.chymeravr.serving.thrift.ServingLog;
import com.chymeravr.thrift.eventreceiver.EventLog;
import lombok.Data;

/**
 * Created by rubbal on 31/1/17.
 */

@Data
public class JoinedEvent {
    private final ServingLog servingLog;
    private final EventLog eventLog;
}
