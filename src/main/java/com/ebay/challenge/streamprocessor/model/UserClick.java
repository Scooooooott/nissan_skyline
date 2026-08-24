package com.ebay.challenge.streamprocessor.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Comparator;
import java.util.TreeSet;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserClick {

    @JsonProperty("clicks")
    private final TreeSet<AdClickEvent> clicks =
            new TreeSet<>(Comparator.comparing(AdClickEvent::getEventTime)
                                    .thenComparing(AdClickEvent::getClickId));

    public synchronized boolean add(AdClickEvent addData){
        return clicks.add(addData);
    }

}
