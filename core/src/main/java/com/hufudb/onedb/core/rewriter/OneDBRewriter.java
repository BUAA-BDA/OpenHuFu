package com.hufudb.onedb.core.rewriter;

import com.hufudb.onedb.core.sql.context.OneDBBinaryContext;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.core.sql.context.OneDBLeafContext;
import com.hufudb.onedb.core.sql.context.OneDBRootContext;
import com.hufudb.onedb.core.sql.context.OneDBUnaryContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface OneDBRewriter {
  static final Logger LOG = LoggerFactory.getLogger(OneDBRewriter.class);

  void rewriteChild(OneDBContext context);
  OneDBContext rewriteRoot(OneDBRootContext root);
  OneDBContext rewriteBianry(OneDBBinaryContext binary);
  OneDBContext rewriteUnary(OneDBUnaryContext unary);
  OneDBContext rewriteLeaf(OneDBLeafContext leaf);
}
