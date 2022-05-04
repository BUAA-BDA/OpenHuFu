package com.hufudb.onedb.core.implementor;

import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.core.sql.context.OneDBContextType;

public abstract class UserSideImplementor implements OneDBImplementor {

  boolean isMultiParty(OneDBContext context) {
    OneDBContextType type = context.getContextType();
    Level level = context.getContextLevel();
    switch(type) {
      case ROOT: // no operation in root context
      case LEAF: // leaf context can be executed on single owner
        return false;
      case UNARY:
      case BINARY:
        // todo: refinement needed
        return !level.equals(Level.PUBLIC);
      default:
        LOG.error("Unsupport context type {}", type);
        throw new UnsupportedOperationException();
    }
  }

  QueryableDataSet ownerSideQuery(OneDBContext context) {
    // todo: send context to owner and get result as queryable dataset
    return null;
  }

  @Override
  public QueryableDataSet implement(OneDBContext context) {
    if (isMultiParty(context)) {
      return ownerSideQuery(context);
    } else {
      // implement on user side
      return context.implement(this);
    }
  }
}
