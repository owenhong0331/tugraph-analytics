SqlCall SqlOptionalMatchPattern():
{
    final SqlNodeList patterns = new SqlNodeList(Span.of());
    final SqlNode pattern;
}
{
    <OPTIONAL> <MATCH>
    pattern = SqlMatchPattern() {
        patterns.add(pattern);
        Span s = Span.of();
        return new SqlOptionalMatchPattern(
            s.end(this),
            new SqlNodeList(patterns, s.addAll(patterns).pos())
        );
    }
}