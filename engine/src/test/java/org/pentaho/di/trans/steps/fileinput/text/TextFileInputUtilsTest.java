/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2021 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.fileinput.text;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.steps.file.BaseFileField;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class TextFileInputUtilsTest {
  @Test
  public void guessStringsFromLine() throws Exception {
    TextFileInputMeta inputMeta = Mockito.mock( TextFileInputMeta.class );
    inputMeta.content = new TextFileInputMeta.Content();
    inputMeta.content.fileType = "CSV";

    String line = "\"\\\\valueA\"|\"valueB\\\\\"|\"val\\\\ueC\""; // "\\valueA"|"valueB\\"|"val\\ueC"

    String[] strings = TextFileInputUtils
      .guessStringsFromLine( Mockito.mock( VariableSpace.class ), Mockito.mock( LogChannelInterface.class ),
        line, inputMeta, "|", "\"", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "\\valueA", strings[ 0 ] );
    Assert.assertEquals( "valueB\\", strings[ 1 ] );
    Assert.assertEquals( "val\\ueC", strings[ 2 ] );
  }

  @Test
  public void convertLineToStrings() throws Exception {
    TextFileInputMeta inputMeta = Mockito.mock( TextFileInputMeta.class );
    inputMeta.content = new TextFileInputMeta.Content();
    inputMeta.content.fileType = "CSV";
    inputMeta.inputFields = new BaseFileField[ 3 ];
    inputMeta.content.escapeCharacter = "\\";

    String line = "\"\\\\fie\\\\l\\dA\"|\"fieldB\\\\\"|\"fie\\\\ldC\""; // ""\\fie\\l\dA"|"fieldB\\"|"Fie\\ldC""

    String[] strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( LogChannelInterface.class ), line, inputMeta, "|", "\"", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "\\fie\\l\\dA", strings[ 0 ] );
    Assert.assertEquals( "fieldB\\", strings[ 1 ] );
    Assert.assertEquals( "fie\\ldC", strings[ 2 ] );
  }

  @Test
  public void convertCSVLinesToStrings() throws Exception {
    TextFileInputMeta inputMeta = Mockito.mock( TextFileInputMeta.class );
    inputMeta.content = new TextFileInputMeta.Content();
    inputMeta.content.fileType = "CSV";
    inputMeta.inputFields = new BaseFileField[ 2 ];
    inputMeta.content.escapeCharacter = "\\";

    String line = "A\\\\,B"; // A\\,B

    String[] strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( LogChannelInterface.class ), line, inputMeta, ",", "", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "A\\", strings[ 0 ] );
    Assert.assertEquals( "B", strings[ 1 ] );

    line = "\\,AB"; // \,AB

    strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( LogChannelInterface.class ), line, inputMeta, ",", "", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( ",AB", strings[ 0 ] );
    Assert.assertEquals( null, strings[ 1 ] );

    line = "\\\\\\,AB"; // \\\,AB

    strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( LogChannelInterface.class ), line, inputMeta, ",", "", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "\\,AB", strings[ 0 ] );
    Assert.assertEquals( null, strings[ 1 ] );

    line = "AB,\\"; // AB,\

    strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( LogChannelInterface.class ), line, inputMeta, ",", "", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "AB", strings[ 0 ] );
    Assert.assertEquals( "\\", strings[ 1 ] );

    line = "AB,\\\\\\"; // AB,\\\

    strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( LogChannelInterface.class ), line, inputMeta, ",", "", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "AB", strings[ 0 ] );
    Assert.assertEquals( "\\\\", strings[ 1 ] );

    line = "A\\B,C"; // A\B,C

    strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( LogChannelInterface.class ), line, inputMeta, ",", "", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "A\\B", strings[ 0 ] );
    Assert.assertEquals( "C", strings[ 1 ] );
  }

  @Test
  public void convertCSVLinesToStringsWithEnclosure() throws Exception {
    TextFileInputMeta inputMeta = Mockito.mock( TextFileInputMeta.class );
    inputMeta.content = new TextFileInputMeta.Content();
    inputMeta.content.fileType = "CSV";
    inputMeta.inputFields = new BaseFileField[ 2 ];
    inputMeta.content.escapeCharacter = "\\";
    inputMeta.content.enclosure = "\"";

    String line = "\"A\\\\\",\"B\""; // "A\\","B"

    String[] strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( LogChannelInterface.class ), line, inputMeta, ",", "\"", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "A\\", strings[ 0 ] );
    Assert.assertEquals( "B", strings[ 1 ] );

    line = "\"\\\\\",\"AB\""; // "\\","AB"

    strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( LogChannelInterface.class ), line, inputMeta, ",", "\"", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "\\", strings[ 0 ] );
    Assert.assertEquals( "AB", strings[ 1 ] );

    line = "\"A\\B\",\"C\""; // "A\B","C"

    strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( LogChannelInterface.class ), line, inputMeta, ",", "\"", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "A\\B", strings[ 0 ] );
    Assert.assertEquals( "C", strings[ 1 ] );
  }

  @Test
  public void getLineWithEnclosureTest() throws Exception {
    String text = "\"firstLine\"\n\"secondLine\"";
    StringBuilder linebuilder = new StringBuilder( "" );
    InputStream is = new ByteArrayInputStream( text.getBytes() );
    BufferedInputStreamReader isr = new BufferedInputStreamReader( new InputStreamReader( is ) );
    TextFileLine line = TextFileInputUtils.getLine( Mockito.mock( LogChannelInterface.class ), isr, EncodingType.SINGLE, 1, linebuilder, "\"", "", 0 );
    Assert.assertEquals( "\"firstLine\"", line.getLine() );
  }

  @Test
  public void getLineBrokenByEnclosureTest() throws Exception {
    String text = "\"firstLine\n\"\"secondLine\"";
    StringBuilder linebuilder = new StringBuilder( "" );
    InputStream is = new ByteArrayInputStream( text.getBytes() );
    BufferedInputStreamReader isr = new BufferedInputStreamReader( new InputStreamReader( is ) );
    TextFileLine line = TextFileInputUtils.getLine( Mockito.mock( LogChannelInterface.class ), isr, EncodingType.SINGLE, 1, linebuilder, "\"", "", 0 );
    Assert.assertEquals( text, line.getLine() );
  }

  @Test
  public void getLineBrokenByEnclosureLenientTest() throws Exception {
    System.setProperty( "KETTLE_COMPATIBILITY_TEXT_FILE_INPUT_USE_LENIENT_ENCLOSURE_HANDLING", "Y" );
    String text = "\"firstLine\n\"\"secondLine\"";
    StringBuilder linebuilder = new StringBuilder( "" );
    InputStream is = new ByteArrayInputStream( text.getBytes() );
    BufferedInputStreamReader isr = new BufferedInputStreamReader( new InputStreamReader( is ) );
    TextFileLine line = TextFileInputUtils.getLine( Mockito.mock( LogChannelInterface.class ), isr, EncodingType.SINGLE, 1, linebuilder, "\"", "", 0 );
    Assert.assertEquals( "\"firstLine", line.getLine() );
    System.clearProperty( "KETTLE_COMPATIBILITY_TEXT_FILE_INPUT_USE_LENIENT_ENCLOSURE_HANDLING" );
  }

  @Test
  public void testCheckPattern() {
    // Check more information in:
    // https://docs.oracle.com/javase/tutorial/essential/regex/literals.html
    String metacharacters = "<([{\\^-=$!|]})?*+.>";
    for( int i = 0; i < metacharacters.length(); i++ ) {
      int matches = TextFileInputUtils.checkPattern( metacharacters, String.valueOf( metacharacters.charAt( i ) ), null );
      Assert.assertEquals( 1, matches );
    }
  }

  @Test
  public void testCheckPatternWithEscapeCharacter() {
    List<String> texts = new ArrayList<>();
    texts.add( "\"valueA\"|\"valueB\\\\\"|\"valueC\"" );
    texts.add( "\"valueA\"|\"va\\\"lueB\"|\"valueC\"" );

    for ( String text : texts ) {
      int matches = TextFileInputUtils.checkPattern( text, "\"", "\\" );
      Assert.assertEquals( 6, matches );
    }

  }

}
