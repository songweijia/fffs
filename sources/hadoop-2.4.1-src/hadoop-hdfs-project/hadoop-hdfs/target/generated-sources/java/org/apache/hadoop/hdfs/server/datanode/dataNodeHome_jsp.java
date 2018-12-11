package org.apache.hadoop.hdfs.server.datanode;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.hdfs.server.datanode.DatanodeJspHelper;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.ServletUtil;

public final class dataNodeHome_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {


  //for java.io.Serializable
  private static final long serialVersionUID = 1L;

  private static java.util.List _jspx_dependants;

  public Object getDependants() {
    return _jspx_dependants;
  }

  public void _jspService(HttpServletRequest request, HttpServletResponse response)
        throws java.io.IOException, ServletException {

    JspFactory _jspxFactory = null;
    PageContext pageContext = null;
    HttpSession session = null;
    ServletContext application = null;
    ServletConfig config = null;
    JspWriter out = null;
    Object page = this;
    JspWriter _jspx_out = null;
    PageContext _jspx_page_context = null;


    try {
      _jspxFactory = JspFactory.getDefaultFactory();
      response.setContentType("text/html; charset=UTF-8");
      pageContext = _jspxFactory.getPageContext(this, request, response,
      			null, true, 8192, true);
      _jspx_page_context = pageContext;
      application = pageContext.getServletContext();
      config = pageContext.getServletConfig();
      session = pageContext.getSession();
      out = pageContext.getOut();
      _jspx_out = out;


/*
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

  
  DataNode dataNode = (DataNode)getServletContext().getAttribute("datanode"); 
  String state = dataNode.isDatanodeUp()?"active":"inactive";
  String dataNodeLabel = dataNode.getDisplayName();

      out.write("<!DOCTYPE html>\n<html>\n<head>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n<title>Hadoop DataNode&nbsp;");
      out.print(dataNodeLabel);
      out.write("</title>\n</head>    \n<body>\n<h1>DataNode '");
      out.print(dataNodeLabel);
      out.write('\'');
      out.write(' ');
      out.write('(');
      out.print(state);
      out.write(")</h1>\n");
      out.print( DatanodeJspHelper.getVersionTable(getServletContext()) );
      out.write("<br />\n<b><a href=\"/logs/\">DataNode Logs</a></b>\n<br />\n<b><a href=\"/logLevel\">View/Set Log Level</a></b>\n<br />\n<b><a href=\"/metrics\">Metrics</a></b>\n<br />\n<b><a href=\"/conf\">Configuration</a></b>\n<br />\n<b><a href=\"/blockScannerReport\">Block Scanner Report</a></b>\n");

out.println(ServletUtil.htmlFooter());

    } catch (Throwable t) {
      if (!(t instanceof SkipPageException)){
        out = _jspx_out;
        if (out != null && out.getBufferSize() != 0)
          out.clearBuffer();
        if (_jspx_page_context != null) _jspx_page_context.handlePageException(t);
      }
    } finally {
      if (_jspxFactory != null) _jspxFactory.releasePageContext(_jspx_page_context);
    }
  }
}
