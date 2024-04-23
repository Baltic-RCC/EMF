<?xml version="1.0" encoding="UTF-8" ?>
<xsl:transform xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
            xmlns="http://entsoe.eu/checks" 
            xmlns:opde="http://example.com/opde" 
            xmlns:pmd="http://example.com/opde" version="1.0">
    <xsl:output method="xml" omit-xml-declaration="no" encoding="UTF-8" exclude-result-prefixes="opde pmd" indent="yes" />
    <xsl:template match="report">
    
    <!--ROOT ELEMENT-->
        <xsl:element name = "QAReport">
            <xsl:attribute name="created" select="current-dateTime()"/>
            <xsl:attribute name="schemeVersion">
                <text>2.0</text>
            </xsl:attribute>
            <xsl:attribute name="serviceProvider">
                <text>BALTICRSC</text>
            </xsl:attribute>
            
            <xsl:for-each select="validation">
            
                <!--<xsl:if test="Region!='ENTSO-E'">-->
                
                <xsl:variable name="LOADFLOW_CONVERGED" select="//validation/loadflow_results/component_0/status = 'CONVERGED'"/>
                <xsl:variable name="TOPOLOGY_CORRECT" select="//validation/valid = 'True'" />
                <xsl:variable name="REPORT_TYPE" select="'IGM'"/>
                    <!--IGM REPORT-->
                    <xsl:element name = "{$REPORT_TYPE}">
                        <xsl:attribute name="created">
                            <xsl:value-of select="//metadata/profile/pmd:file-uploaded-time"/>
                        </xsl:attribute>
                        <xsl:attribute name="processType">
                            <xsl:value-of select="//metadata/profile/pmd:timeHorizon"/>
                        </xsl:attribute>
                        <xsl:attribute name="qualityIndicator">
                        <xsl:choose>
                            <xsl:when test="$LOADFLOW_CONVERGED and $TOPOLOGY_CORRECT">
                                <text>Plausible</text>
                            </xsl:when>
                            <xsl:otherwise>
                                <text>Substituted</text>
                            </xsl:otherwise>
                        </xsl:choose>
                        </xsl:attribute>
                        <xsl:attribute name="scenarioTime">
                            <xsl:value-of select="//metadata/profile/pmd:scenarioDate"/>
                        </xsl:attribute>
                        <xsl:variable name="MAS" select="//metadata/profile/pmd:modelingAuthoritySet" />
                        <xsl:attribute name="tso">
                            <xsl:value-of select="//metadata/profile/pmd:sourcingActor"/>
                        </xsl:attribute>
                        <xsl:attribute name="version">
                            <xsl:value-of select="//metadata/profile/pmd:version"/>
                        </xsl:attribute>

                        <!--REFERENCES-->
                        <xsl:for-each select = "//metadata/profile/opde:Context/opde:EDXContext">
                            <xsl:element name = "resource">
                                <xsl:value-of select="opde:EDXBaCorrelationID"/>
                            </xsl:element>
                        </xsl:for-each>

                        <!--ERRORS-->
                    	<xsl:if test="not($LOADFLOW_CONVERGED)">
                            <xsl:element name="RuleViolation">
                                <xsl:attribute name="validationLevel"><xsl:text>8</xsl:text></xsl:attribute>
                                <xsl:attribute name="ruleId"><xsl:text>IGMConvergence</xsl:text></xsl:attribute>
                                <xsl:attribute name="severity">ERROR</xsl:attribute>
                                <xsl:element name="Message">MSG 52. ERROR: IGM for <xsl:value-of select="$MAS"/> did not converge.</xsl:element>
                            </xsl:element>

						</xsl:if>
                        
                        <xsl:if test="not($TOPOLOGY_CORRECT)">
                            <xsl:for-each select="//validation/validations">
                                <xsl:if test="BUSES='False'">
                                <xsl:element name="RuleViolation">
                                <xsl:attribute name="validationLevel"><xsl:text>8</xsl:text></xsl:attribute>
                                <xsl:attribute name="ruleId"><xsl:text>WrongTopology</xsl:text></xsl:attribute>
                                <xsl:attribute name="severity"><xsl:text>ERROR</xsl:text></xsl:attribute>
                                <xsl:element name="Message">TOPOLOGY ERROR: Wrong topology at type</xsl:element>
                            </xsl:element>

                        </xsl:if>
                        </xsl:for-each>
                        </xsl:if>
                                              
                    </xsl:element>
               <!-- </xsl:if> -->
               
            </xsl:for-each>
                          
        </xsl:element>
        
    </xsl:template>
</xsl:transform>
