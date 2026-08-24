package com.ebay.challenge.streamprocessor.output;

import com.ebay.challenge.streamprocessor.infrastructure.OutputSinkException;
import com.ebay.challenge.streamprocessor.mapper.OutputMapper;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import io.micrometer.common.util.StringUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ObjectUtils;


@Slf4j
@Component
@RequiredArgsConstructor
public class OutputSink {

    private final OutputMapper outputMapper;

    /**
     * Persist a finalized attributed page view.
     * <p>
     * The operation must be idempotent by pageViewId.
     * A successful return means the output has been durably persisted.
     *
     * @param record finalized attribution result
     * @throws OutputSinkException if persistence fails or an
     *                             inconsistent duplicate is detected
     */
    @Transactional
    public void write(AttributedPageView record) {
        validate(record);
        try {
            boolean inserted = outputMapper.insertIfAbsent(record);

            // insert record
            if (inserted) {
                log.debug("OutputSink.write: Attributed page view stored: pageViewId={}", record.getPageViewId());
                return;
            }

            // if record can not be inserted, check if duplicated
            AttributedPageView existRecord = outputMapper.findByPageViewId(record.getPageViewId());

            // same info duplicated insert, just warning, no error reported
            if (record.equals(existRecord)) {
                log.debug("utputSink.write: Attributed page view already exists: pageViewId={}", record.getPageViewId());
                return;
            }

            // different info for same pageViewId, throw an exception
            String errMsg = "Output conflict for pageViewId:" + record.getPageViewId()
                    + ". Existing record: " + existRecord + ", incoming record:" + record;
            log.error("OutputSink.write: {}", errMsg);
            throw new OutputSinkException(errMsg);
        } catch (OutputSinkException e) {
            throw e;
        } catch (RuntimeException e) {
            String errMsg = "Unexpected error while inserting attributed page view to db: pageViewId: " + record.getPageViewId();
            log.error("OutputSink.write: {}{}", errMsg, e.getMessage());
            throw new OutputSinkException(errMsg, e);
        }
    }


    /**
     * validate for inserting
     * <p>
     * 1. record and some fields must be not blank
     * 2. attributed-related fields must be both blank or both not blank
     * */
    private void validate(AttributedPageView record) {
        if ((record == null) || (StringUtils.isBlank(record.getPageViewId()))
        || (StringUtils.isBlank(record.getUserId()) || StringUtils.isBlank(record.getUrl()))
        || ObjectUtils.isEmpty(record.getEventTime())
        ) {
            // record might be null or with no page_view_id, so use record instead of pageViewId here
            throw new OutputSinkException("OutputSink.write: Cannot persist a null attributed page view, record:" + record);
        }

        if ((StringUtils.isBlank(record.getAttributedCampaignId()) && StringUtils.isBlank(record.getAttributedClickId()))
        || (!StringUtils.isBlank(record.getAttributedCampaignId()) && !StringUtils.isBlank(record.getAttributedClickId()))){
            throw new OutputSinkException("OutputSink.write: Attribution fields must be both null or both present, pageViewId:" + record.getPageViewId());
        }
    }


}
