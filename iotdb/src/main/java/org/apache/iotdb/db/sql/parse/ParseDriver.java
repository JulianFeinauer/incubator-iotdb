/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.sql.parse;

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.NoViableAltException;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ParseDriver.
 *
 */
public class ParseDriver {

  /**
   * Tree adaptor for making antlr return AstNodes instead of CommonTree nodes so that the graph walking algorithms
   * and the rules framework defined in ql.lib can be used with the AST Nodes.
   */
  public static final TreeAdaptor adaptor = new CommonTreeAdaptor() {
    /**
     * Creates an AstNode for the given token. The AstNode is a wrapper around antlr's CommonTree class that
     * implements the Node interface.
     *
     * @param payload
     *            The token.
     * @return Object (which is actually an AstNode) for the token.
     */
    @Override
    public Object create(Token payload) {
      return new AstNode(payload);
    }

    @Override
    public Object dupNode(Object t) {

      return create(((CommonTree) t).token);
    }

    @Override
    public Object errorNode(TokenStream input, Token start, Token stop, RecognitionException e) {
      return new AstErrorNode(input, start, stop, e);
    }

  };
  private static final Logger LOG = LoggerFactory.getLogger("ql.parse.ParseDriver");

  /**
   * Parses a command, optionally assigning the parser's token stream to the given context.
   *
   * @param command
   *            command to parse
   *
   * @return parsed AST
   */
  public AstNode parse(String command) throws ParseException {
    TSLexerX lexer = new TSLexerX(new ANTLRNoCaseStringStream(command));
    TokenRewriteStream tokens = new TokenRewriteStream(lexer);

    TSParser parser = new TSParser(tokens);

    parser.setTreeAdaptor(adaptor);
    TSParser.statement_return r = null;

    try {

      r = parser.statement();
    } catch (RecognitionException e) {
      LOG.trace("meet error while parsing statement: {}", command, e);
    }

    if (!lexer.getErrors().isEmpty()) {
      throw new ParseException(lexer.getErrors());
    }
    if (!parser.errors.isEmpty()) {
      throw new ParseException(parser.errors);
    }

    if (r != null) {
      AstNode tree = (AstNode) r.getTree();
      tree.setUnknownTokenBoundaries();
      return tree;
    } else {
      return null;
    }
  }

  /**
   * ANTLRNoCaseStringStream.
   *    This class provides and implementation for a case insensitive token checker
   *    for the lexical analysis part of antlr. By converting the token stream into
   *    upper case at the time when lexical rules are checked, this class ensures that the
   *    lexical rules need to just match the token with upper case letters as opposed to
   *    combination of upper case and lower case characteres. This is purely used for matching lexical
   *    rules. The actual token text is stored in the same way as the user input without
   *    actually converting it into an upper case. The token values are generated by the consume()
   *    function of the super class ANTLRStringStream. The LA() function is the lookahead funtion
   *    and is purely used for matching lexical rules. This also means that the grammar will only
   *    accept capitalized tokens in case it is run from other tools like antlrworks which
   *    do not have the ANTLRNoCaseStringStream implementation.
   */
  public class ANTLRNoCaseStringStream extends ANTLRStringStream {

    public ANTLRNoCaseStringStream(String input) {
      super(input);
    }

    @Override
    public int LA(int i) {

      int returnChar = super.LA(i);
      if (returnChar == CharStream.EOF || returnChar == 0) {
        return returnChar;
      }

      return Character.toUpperCase((char) returnChar);
    }
  }

  /**
   * TSLexerX.
   *
   */
  public class TSLexerX extends TSLexer {

    private final ArrayList<ParseError> errors;

    public TSLexerX(CharStream input) {
      super(input);
      errors = new ArrayList<>();
    }

    @Override
    public void displayRecognitionError(String[] tokenNames, RecognitionException e) {

      errors.add(new ParseError(this, e, tokenNames));
    }

    @Override
    public String getErrorMessage(RecognitionException e, String[] tokenNames) {
      String msg;

      if (e instanceof NoViableAltException) {
        // for development, can add
        // "decision=<<"+nvae.grammarDecisionDescription+">>"
        // and "(decision="+nvae.decisionNumber+") and
        // "state "+nvae.stateNumber
        msg = "character " + getCharErrorDisplay(e.c) + " not supported here";
      } else {
        msg = super.getErrorMessage(e, tokenNames);
      }
      String input = e.input.toString();
      if (input.contains("timestamp") || input.contains("time")) {
        msg += ". (Note that time format should be something like 1) number: eg.123456 2) function: eg.now() 3) datatime: eg.yyyy-MM-dd HH:mm:ss, please check whether inputting correct time format or referring to sql document)";
      }
      return msg;
    }

    public List<ParseError> getErrors() {
      return errors;
    }

  }

}
