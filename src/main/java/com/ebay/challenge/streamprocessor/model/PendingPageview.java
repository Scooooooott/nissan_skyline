package com.ebay.challenge.streamprocessor.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Comparator;
import java.util.TreeSet;

@Data
@NoArgsConstructor
public class PendingPageview {

    @JsonProperty("pageviews")
    private final TreeSet<PageViewEvent> pageviews =
            new TreeSet<>(Comparator.comparing(PageViewEvent::getEventTime)
                                    .thenComparing(PageViewEvent::getEventId));

    public synchronized boolean add(PageViewEvent addData){
        return pageviews.add(addData);
    }

}
