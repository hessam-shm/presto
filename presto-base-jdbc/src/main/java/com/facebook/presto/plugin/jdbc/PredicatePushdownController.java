package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.common.predicate.DiscreteValues;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.EquatableValueSet;
import com.facebook.presto.common.predicate.Ranges;
import com.facebook.presto.spi.ConnectorSession;

import static java.util.Objects.requireNonNull;

public interface PredicatePushdownController {

    PredicatePushdownController FULL_PUSHDOWN = (session, domain) -> {
        if (getDomainSize(domain) > getDomainCompactionThreshold(session)) {
            // pushdown simplified domain
            return new DomainPushdownResult(domain.simplify(), domain);
        }
        return new DomainPushdownResult(domain, Domain.all(domain.getType()));
    };

    static int getDomainCompactionThreshold(ConnectorSession session) {
        return 1000000;
    }

    PredicatePushdownController PUSHDOWN_AND_KEEP = (session, domain) -> new DomainPushdownResult(
            domain.simplify(),
            domain);
    PredicatePushdownController DISABLE_PUSHDOWN = (session, domain) -> new DomainPushdownResult(
            Domain.all(domain.getType()),
            domain);

    DomainPushdownResult apply(ConnectorSession session, Domain domain);

    final class DomainPushdownResult
    {
        private final Domain pushedDown;
        // In some cases, remainingFilter can be the same as pushedDown, e.g. when target database is case insensitive
        private final Domain remainingFilter;

        public DomainPushdownResult(Domain pushedDown, Domain remainingFilter)
        {
            this.pushedDown = requireNonNull(pushedDown, "pushedDown is null");
            this.remainingFilter = requireNonNull(remainingFilter, "remainingFilter is null");
        }

        public Domain getPushedDown()
        {
            return pushedDown;
        }

        public Domain getRemainingFilter()
        {
            return remainingFilter;
        }
    }

    static int getDomainSize(Domain domain)
    {
        return domain.getValues().getValuesProcessor().transform(
                Ranges::getRangeCount,
                EquatableValueSet::getValuesCount,
                ignored -> 0);
    }
}
