package datadog.trace.core.processor.rule;

import datadog.trace.api.DDTags;
import datadog.trace.core.DDSpan;
import datadog.trace.core.processor.TraceProcessor;
import java.util.Collection;
import java.util.Map;

/** Converts span type tag to field */
public class SpanTypeRule implements TraceProcessor.Rule {
  @Override
  public String[] aliases() {
    return new String[] {"SpanTypeDecorator"};
  }

  @Override
  public void processSpan(
      final DDSpan span, final Map<String, Object> tags, final Collection<DDSpan> trace) {
    final Object type = tags.get(DDTags.SPAN_TYPE);
    if (type != null) {
      span.setSpanType(type.toString());
    }

    span.removeTag(DDTags.SPAN_TYPE);
  }
}
